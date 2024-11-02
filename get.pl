#!/usr/bin/perl
#!/home/sfegan/bin/perl

#
# Transfer data files from the acquisition computer to the appropriate
# directory on the quicklook computer. Written because DH was too lazy
# to type "ftp purav3" followed by "get gt0?????.fz". SJF 990306
#

use integer;
use strict;

use Net::FTP;
use Net::Netrc;
use FileHandle;

sub GetUT;
sub Connect;
sub TransferFile;

my $RemoteHost='taurus.sao.arizona.edu';	#purav3.sao.arizona.edu';
my $RemoteUser='observer';
my $RemotePass='XXXXXXXXXx';
my $RemoteDiry='/dir$data';

my $LocalDir='/draco/raw10';

my $ftp=Connect($RemoteHost,$RemoteUser,$RemotePass);
exit unless $ftp;

print STDERR 'Changing directory to "',$RemoteDiry,'"',"\n";
unless($ftp->cwd($RemoteDiry))
  {
    print STDERR "Could not change directory\n";
    exit;
  }

my $UT=GetUT;
print STDERR 'Using ',$UT,' as current UT date',"\n";

system("mkdir ".$LocalDir."/d".$UT) unless ( -d $LocalDir."/d".$UT );

my @RunNoList=UnBunchNos(join(", ",@ARGV));
my $Run;

print STDERR ("Getting run",((scalar(@RunNoList)>1)?"s":""),": ", 
	join(",",@RunNoList),"\n");

foreach $Run ( @RunNoList )
  {
    my $RemoteFile="GT".sprintf("%6.6d",$Run).".FZ";
    my $LocalFile=$LocalDir."/d".$UT."/gt".sprintf("%6.6d",$Run).".fz";
    
    print(STDERR "Transferring ",$RemoteFile," -> ",$LocalFile,"\n"); 
    TransferFile($ftp,$RemoteFile,$LocalFile);
  }

#print join("\n",$ftp->dir),"\n";

sub TransferFile
  {
    my $ftp=shift;
    my $RemoteFile=shift;
    my $LocalFile=shift;
    
    my $Buffer;
    my $connection;
    
    $connection=$ftp->retr($RemoteFile);
    
    if($connection)
      {
	my $FH=new FileHandle($LocalFile,"w");
	my $size=0;
	my $time=time;
	my $read;
	
	if($FH)
	  {
	    while(($read=$connection->read($Buffer,512000)))
	      {
		$FH->print($Buffer);
		print STDERR "." if(($size+$read)/512000 != ($size/512000));
		$size+=$read;
	      }
	    $time=time-$time;
	    print(STDERR "\nGot ",$size/1024," kB in ",$time," seconds, ",
		  $size/($time*1024)," kB/sec\n");
	    undef $FH;
	  }
	else
	  {
	    $connection->abort;
	    print STDERR "Could not open ",$LocalFile,": ",$!,"\n";
	  }
	
	$connection->close;
	undef $connection;
      }
    else
      {
	print STDERR "Could not open a connection\n";
      }
  }

#
# Connect to the remote computer, search for a .netrc entry and log in
#
sub Connect
  {
    my $Host=shift;
    my $User=shift;
    my $Pass=shift;
    
    print STDERR "Connect to ",$Host;
    
    my $ftp=Net::FTP->new($RemoteHost);
    unless($ftp)
      {
	print STDERR "\n";
	print STDERR "Could not connect: ",$@,"\n";
      }
    
    my $Netrc=Net::Netrc->lookup($Host);
    ($User, $Pass)=$Netrc->lpa if($Netrc);
    
    print STDERR ' as "',$RemoteUser,'" ';
    print STDERR 'pass "','*' x length($RemotePass),'"';
    print STDERR ' (netrc)' if $Netrc;
    print STDERR "\n";
    
    unless($ftp->login($RemoteUser,$RemotePass))
      {
	print STDERR "Could not log in, exiting\n";
	exit;
      }
    
    $ftp->binary;
    
    return $ftp;
  }

#
# Figure out the current UT date
#
sub GetUT
  {
    my $UT;
    my $time=time;
    my @datecomponents=gmtime;
    my $UT=sprintf("%2.2d%2.2d%2.2d",$datecomponents[5]%100,
		   $datecomponents[4]+1,$datecomponents[3]);
    return $UT;
  }

#
# Translate strings like 11235-11240 to a list of numbers
#
sub UnBunchNos
  {
    my $RunNos=shift;
    my @UnBunched=split(/\s*([-,\s;])\s*/,$RunNos);
    my @Runs;
    my ($r,$op);
    
    while($r=shift(@UnBunched))
      {
	next unless $r=~/^\d+(\/\d+)*$/;
	
	$op=shift(@UnBunched);
	if((not $op)or($op eq ";")or($op eq ",")or($op=~/\s/))
	  {
	    if ( $r =~ /\// )
	      {
		my ($R,@partials)=split /\//,$r;
		push @Runs,$R;
		foreach ( @partials )
		  {
		    substr($R,length($R)-length($_),length($_))=$_;
		    push @Runs,$R;
		  }
	      }
	    else
	      {
		push @Runs,$r;
	      }
	  }
	elsif($op eq "-")
	  {
	    my $f=shift(@UnBunched);
	    if($f=~/^\d+$/)
	      {
		push @Runs,$r++ while($r<=$f);
	      }
	    $op=shift(@UnBunched);
						    }
      }
    
    return sort({ $a <=> $b } @Runs);
  }
