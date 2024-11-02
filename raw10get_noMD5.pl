#!/usr/bin/perl -w

#
# Program: raw10get.pl
#
# Author: Stephen Fegan, September 2000
#
# Purpose: Download data from the VERITAS server. Can be used to download a
#          whole nights data or just select runs. If the user wants to get a
#          full nights data then we get a list of files (called the contents
#          from the server first). Can also just show how much space is needed
#          to fullfill the request. When files are download we check that thir
#          size is as expeced and that they have the correct (MD5) checksum.
#          On some early RedHat systems the MD5 module for Perl is not included
#          so there is a version of this without the checksum calculation. The
#          download is done by http from the server from withing this script,
#          the first incarnation of this script used lynx to download files
#          but it seems that most versions of lynx were actually broken. This
#          version does the download itself using HTTP/1.1 which should be
#          faster anyway since only one connection to the server need be opened
#          (lynx had to open one for each file)
#

use strict;

use File::stat;
use FileHandle;
use IO::Socket;

use Data::Dumper;

###############################################################################
################### PARAMETERS THAT YOU MAY NEED TO ADJUST ####################
###############################################################################

my $target_base_dir    = '/tmp';

my $server_base_url    = 'http://veritas.sao.arizona.edu/raw10';
my $server_user        = 'observer';
my $server_pass        = 'XXXXXXXXXXXX';

my $comm_attempts      = 5;                   # number of time to try download

my $contents_file      = 'contents';          # standard name of contents file

###############################################################################
############################# FORWARD DECLARATIONS ############################
###############################################################################

sub test_file_equals;        # see if a local copy of the file exists

sub get_contents;            # get the contents for a night
sub get_remote_contents;     # initiate the transfer and parse results

sub download_summary;        # show how much space things will take

sub download_files;          # download a group of files
sub get_file;                # get a file (maybe trying a few times)
sub get_remote_file;         # initiate file transfer and save output

sub get_remote_file_handle;  # start the communicator .. used to be lynx
                             # we now just do things ourselves as lynx is bad

sub target_filename;         # compose the file name for the target file
sub server_filename;         # compose the file url for the server file

sub usage;

sub main;

###############################################################################
############################# VARIABLE DEFINITIONS ############################
###############################################################################

my $verbose            = 0;

###############################################################################
################################ MAIN FUNCTION ################################
###############################################################################

main;

sub main
  {
    my @REQ_UT             = ();  # requested dates to download
    my @REQ_RUNS           = ();  # requested runs to download
    
    my %Contents           = ();  # list of nights we need contents for
    my %Files              = ();  # list of files to download
    
    my $summary_only       = 0;
    
    #
    # PROCESS COMMAND LINE
    #
    
    my $arg;
    while(defined($arg=shift(@ARGV)))
      {
	if ( $arg =~ s/^-// )
	  {
	    # This argument starts with a '-' so it is treated as an option.
	    while ( $arg =~ s/^(.)// )
	      {
		if ( $1 eq 's' ) { $summary_only=(($summary_only==1)?0:1); 
				   next; }
		if ( $1 eq 'v' ) { $verbose=(($verbose==1)?0:1); next; }
		if ( $1 eq 'h' ) { usage; }
		if ( $1 eq 'd' ) { if($arg) { $target_base_dir=$arg; $arg="" }
				   else { $target_base_dir=shift(@ARGV) } 
				   next; }
		print($0,": Unrecognised option: ",$1,"\n\n");
		usage;
	      }
	  }
	elsif ( $arg =~ /^d?(\d{6})$/ ) 
	  {
	    # If the arguement is composed of 6 digits (possible prefixed
	    # by a d) then presume that it is a UT date.
	    push @REQ_UT,$1;
	  }
	elsif ( $arg =~ /^d?(\d{6})\/(.*)$/ ) 
	  {
	    # If it looks like dYYMMDD/RUNNO then just get that one run
	    my $ut=$1;
	    my $run=$2;
	    $run=sprintf("gt%6.6d.fz.bz2",$run) if ( $run =~ /^\d+$/ );
	    push @REQ_RUNS,[$ut,$run];
	  }
	else 
	  { 
	    # Otherwise presume its the name of a file to open and get a
	    # list of runs from
	    my $handle;
	    if((-r $arg) && (defined ($handle=new FileHandle $arg,"r")))
	      {
		# We can read the file and it opened properly. Read each line
		# and add it to the list of runs to download
		my $line;
		while(defined ($line = $handle->getline))
		  {
		    $line =~ s/^\s*//;  # strip leading space
		    $line =~ s/\#.*//;  # ignore comments
		    $line =~ s/\s*$//;  # strip trailing space
		    
		    next if ( not $line );
		    
		    my ($ut,$run,$size,$md5)=split(/\s+/,$line);
		    
		    if( $ut =~ /^d?(\d{6})$/ ) # only things like d000101 etc
		      {
			$ut = $1;
		      }
		    else 
		      { 
			next;
		      }
		    
		    $run=sprintf("gt%6.6d,fz.bz2",$run) if ( $run =~ /^\d+$/ );
		    
		    if(defined($size) && defined($md5) && 
		       $size =~ /^\d+$/ && $md5 =~ /[1-90a-fA-F]{32}/ )
		      {
			push @REQ_RUNS,[$ut,$run,$size,$md5];
		      }
		    else
		      {
			push @REQ_RUNS,[$ut,$run];
		      }
		  }
	      }
	    else
	      {
		print STDERR ($0,": Could not open file ",$arg,"\n");
	      }
	  }
      }
    
    #
    # Build list of contents files to download
    #
    
    foreach ( @REQ_UT ) { $Contents{$_} = [] };
    foreach ( @REQ_RUNS ) { $Contents{$_->[0]} = [] 
			      if ( not defined($_->[2]) ); };
    
    #
    # Loop through each night, download the contents file from the server 
    # for those nights requested 
    #
    
    my $UT;
    foreach $UT ( sort keys %Contents )
      {
	print STDERR $UT,": Getting contents file from server...\n";
	my $files=get_contents($UT);
	if( (not defined($files)) || (not ref($files)) )
	  {
	    print STDERR $UT,": Could not get contents.. skipping this day\n";
	    next;
	  }
	$Contents{$UT}=$files;
      }
    
    #
    # create the list of files to download
    #
    
    # firstly the files for any full days we are to download. take the 
    # file list directly from the contents file
    foreach $UT ( @REQ_UT ) 
      { 
	my $fileentry;
	
	next unless ( (exists($Contents{$UT})) && (ref($Contents{$UT})) );
	
	foreach $fileentry ( @{$Contents{$UT}} )
	  {
	    $Files{$UT}->{$fileentry->[1]}=$fileentry;
	  }
      }
    
    # next any additional files to download
    my $fileentry;
    foreach $fileentry ( @REQ_RUNS )
      {
	my $ut=$fileentry->[0];
	my $filename=$fileentry->[1];
	
	my $found_fileentry;
	
	if(defined $fileentry->[2])
	  {
	    # user has given us size / md5
	    $found_fileentry=$fileentry;
	  }
	else
	  {
	    my $cfileentry;
	    foreach $cfileentry ( @{$Contents{$ut}} )
	      {
		next if ($cfileentry->[1] ne $filename);
		$found_fileentry=$cfileentry;
		last;
	      }
	  }
	
	if(not defined $found_fileentry)
	  {
	    print STDERR $ut,": Run ",$filename," not found.. skipping\n";
	    next;
	  }
	
	$Files{$ut}->{$filename}=$found_fileentry;
      }
    
    if($summary_only)
      {
	download_summary(\%Files);
	exit;
      }
    
    # start downloading....
    download_files(\%Files);
  }

###############################################################################
############################# DOWNLOAD SUMMARY ################################
###############################################################################

sub download_summary
  {
    my $Files=shift;

    my $total_kbytes=0;

    print (("-"x79),"\n");

    my $UT;
    foreach $UT ( sort { $a <=> $b } keys %{$Files} )
      {
	my $Dir=target_filename($UT);

	my $UTFiles=$Files->{$UT};
	my $file;

	my $kbytes=0;

	foreach $file ( sort { $a cmp $b } keys %{$UTFiles} )
	  {
	    my $fileentry=$UTFiles->{$file};
	    my $r_size=$fileentry->[2];
	    my $r_md5sum=$fileentry->[3];

	    $kbytes += int(($r_size+1024-1)/1024);
	  }

	print join(" ",$Dir,"-",$kbytes,"kB"),"\n";
	$total_kbytes+=$kbytes;
      }

    my $units="kB";
    if($total_kbytes>4096)
      {
	$total_kbytes = int(($total_kbytes+1024-1)/1024);
	$units="MB";
      }	
    
    print "\n",join(" ","Total requirement",$total_kbytes,$units),"\n";
    print (("-"x79),"\n");
  }

###############################################################################
############### DOWNLOAD FILES, FIRST CHECKING WHETHER THEY MATCH #############
###############################################################################

sub download_files
  {
    my $Files=shift;

    my $UT;
    foreach $UT ( sort { $a <=> $b } keys %{$Files} )
      {
	my $Dir=target_filename($UT);

	if(not -e $Dir)
	  {
	    print STDERR $UT,": Making directory: ",$Dir,"\n";
	    system qq{mkdir $Dir};
	  }
	
	my $UTFiles=$Files->{$UT};
	my $file;

	foreach $file ( sort { $a cmp $b } keys %{$UTFiles} )
	  {
	    my $fileentry=$UTFiles->{$file};
	    my $r_size=$fileentry->[2];
	    my $r_md5sum=$fileentry->[3];

	    my $target=target_filename($UT,$file);
	    if(test_file_equals($UT,$target,$r_size,$r_md5sum) == 1)
	      {
		print STDERR ($UT,": Verified file ",$file,
			      " was already downloaded correctly\n");
		next;
	      }
	    
	    print STDERR ($UT,": Getting file ",$file," (",int($r_size/1024),
			  " kb)\n");

	    my $success=get_file($UT,$fileentry,$Dir);
	    if((defined $success) && ($success == 1))
	      {
		print STDERR $UT,": Download of ",$file," successful\n";
	      }
	    else
	      {
		print STDERR $UT,": Could not download ",$file,"\n";
	      }
	  }
      }
  }

###############################################################################
########################### DO THE ACTUAL DOWNLOAD ############################
###############################################################################

sub get_file
  {
    my $UT=shift;
    my $fileentry=shift;

    my $target_dir=shift;

    my $file=$fileentry->[1];
    my $r_size=$fileentry->[2];
    my $r_md5sum=$fileentry->[3];
    
    my $attempt=0;
    my $success=0;
    
    while($attempt<$comm_attempts)
      {
	$attempt++;

	print STDERR ( $UT,": DOWNLOAD: downloaded file does not match...",
		       "trying again\n" ) if ( $attempt != 1 );

	print STDERR ( $UT,": DOWNLOAD: ",$file," attempt ",$attempt,"/",
		       $comm_attempts,"\n" )
	  if ($verbose);
	
	my $ofilename=join("/",$target_dir,$file);
	my $ohandle=new FileHandle $ofilename,"w";
	if(not defined $ohandle)
	  {
	    print STDERR ( $UT,": DOWNLOAD: could not open output file ",
			   $file,"\n" );
	    print STDERR ( $UT,": DOWNLOAD: ERROR: ",$!,"\n" );
	    return undef;
	  }
	
	my $url=server_filename($UT,$file);
	my $ihandle=get_remote_file_handle($UT,$url,$server_user,$server_pass);
	if(not defined $ihandle)
	  {
	    print STDERR ( $UT,": DOWNLOAD: could not open connection to",
			   " server\n" );
	    return undef;
	  }

	if(not ref $ihandle)
	  {
	    print STDERR ( $UT,": DOWNLOAD: server did not return file ",
			   $file,"\n" );
	    return undef;
	  }

	my $dots=0;
	my $bytestransferred=0;
	print STDERR $UT,": DOWNLOAD: ";

	while(1)
 	  {
	    my $buffer;
	    my $bytesread=$ihandle->read(\$buffer,100000);
	    last if ($bytesread==0);

	    if(not defined $ohandle->print($buffer))
	      {
		print STDERR "\n",$UT,": DOWNLOAD: write error ",$file,": $!";
		return undef;
	      }

	    $bytestransferred+=$bytesread;
	    my $hdots=int(($bytestransferred*60)/$r_size);
	    if($hdots != $dots)
	      {
		print STDERR ("=" x ($hdots-$dots));
		$dots=$hdots;
	      }
	  }

	undef $ihandle;
	undef $ohandle;

	print STDERR "\n";

	if(test_file_equals($UT,$ofilename,$r_size,$r_md5sum) == 1)
	  {
	    $success=1;
	    last;
	  }
      }

    print STDERR ( $UT,": DOWNLOAD: maximum number of attempts reached\n" )
      if ( $success==0 );
    
    return $success;
  }


###############################################################################
########################## GET THE NIGHTLY CONTENTS ###########################
###############################################################################

sub get_contents
  {
    my $UT=shift;

    my $attempt=0;

    while($attempt<$comm_attempts)
      {
	$attempt++;
	print STDERR $UT,": CONTENTS: attempt ",$attempt,"\n" if ($verbose);

	my $files=get_remote_contents($UT);
	if(not defined($files))
	  {
	    return  undef;
	  }
	
	if($files == 0)
	  {
	    print STDERR $UT,": CONTENTS: attempt ",$attempt," failed\n"
	      if ($verbose);
	    print STDERR $UT,": CONTENTS: sleeping for 3 seconds\n"
	      if ($verbose);
	    sleep(3);
	    next;
	  }

	return $files;
      }

    print STDERR $UT,": CONTENTS: maximum number of download attempts\n";
    return undef;
  }
 	
sub get_remote_contents
  {
    my $UT=shift;

    my $utdir="d".$UT;
    my $url=join("/",$server_base_url,$utdir,$contents_file);

    my $handle=get_remote_file_handle($UT,$url,$server_user,$server_pass);
    if(not defined $ handle)
      {
	print STDERR ( $UT,
		       ": CONTENTS: could not open connection to server\n" );
	return undef;
      }

    if(not ref $handle)
      {
	print STDERR ( $UT,
		       ": CONTENTS: server did not return contents file\n");
	return undef;
      }
    
    my @Files;

    my $found_start=0;
    my $found_end=0;

    my $line;
    while(defined($line=$handle->getline))
      {
	print $UT,": CONTENTS: ",$line if($verbose);
	chomp $line;

	if($found_start==0)
	  {
	    # Search for the start line of the contents file and make sure
	    # it matches the date we want
	    if($line =~ /START VERITAS DATA CONTENTS FILE (\d{6})/)
	      {
		if($1 != $UT)
		  {
		    print STDERR ($UT,": Contents file mismatch, ",
				  $1,"!=",$UT,"\n");
		    return undef;
		  }
		$found_start=1;
	      }
	  }
	else
	  {
	    # If we find the end tag then skip whatever comes next
	    if($line =~ /END VERITAS DATA CONTENTS FILE (\d{6})/)
	      {
		while(defined($line=$handle->getline)) {}
		$found_end=1;
		next;
	      }
	    
	    push @Files,[$UT,split(/\s+/,$line)];
	  }
      }

    if($found_start == 0)
      {
	print STDERR $UT,": End of contents file... no START tag found\n"
	  if( $verbose );
	return 0;
      }

    if($found_end == 0)
      {
	print STDERR $UT,": End of contents file... no END tag found\n"
	  if( $verbose );
	return 0;
      }
    
    return \@Files;
  }

###############################################################################
################# SPAWN THE COMMUNICATOR AND RETURN A FILEHANDLE ##############
###############################################################################

my $cached_connection  = undef;
my $cached_handle      = undef;

sub reset_handle
  {
    undef $cached_handle;
  }

sub get_remote_file_handle
  {
    my $UT=shift;
    my $url=shift;
    my $user=shift;
    my $pass=shift;

    if ( not $url =~ m{//([^/]*)} )
      {
	print STDERR $UT,": COMM: Attempt to get remote file ",$url," \n";
	print STDERR $UT,": COMM: Cannot figure out host name!!\n";
	return undef;
      }	
    my $server_host=$1;

    my $connection_url;
    my $connection;
    my $remotefile;
    
    if((exists $ENV{'http_proxy'})||(exists $ENV{'HTTP_PROXY'}))
      {
	# We have to use a proxy to get the data
	$connection_url = $ENV{'http_proxy'} if(exists $ENV{'http_proxy'});
	$connection_url = $ENV{'HTTP_PROXY'} if(exists $ENV{'HTTP_PROXY'});
	
	if ( not $connection_url =~ m{^http://([^/]*)} )
	  {
	    print STDERR ( $UT,": COMM: Attempt to use proxy ",
			   $connection_url," \n" );
	    print STDERR $UT,": COMM: Can only handle at HTTP proxies!!\n";
	    return undef;
	  }
	
	$connection = $1;
	$remotefile=$url;
      }
    else
      {
	# Go directly to the server
	$connection_url = $url;
	if ( not $connection_url =~ m{^http://([^/]*)(/.*)} )
	  {
	    print STDERR ( $UT,": COMM: Attempt to get remote file ",
			   $connection_url," \n" );
	    print STDERR $UT,": COMM: Can only handle at HTTP requests!!\n";
	    return undef;
	  }
	
	$connection=$1;
	$remotefile=$2;
      }
    
    my $host;
    my $port;
 
    if($connection =~ m/(.+):(\d+)/)
      {
	$host = $1;
	$port = $2;
      }
    else
      {
	$host = $connection;
	$port = 80;
      }

    my $HTTPversion="1.1";

    if((not defined $cached_connection)||($cached_connection ne $connection))
      {
	undef $cached_handle;
	$cached_connection  = $connection;
      }

    # If we have a cached socket handle but it isn't open 
    # then we should not use it again
    undef $cached_handle
      if((defined $cached_handle) && (not $cached_handle->opened));
    
    while(1)
      {
	my $comm_handle = $cached_handle;
	if(not defined $comm_handle)
	  {
	    $comm_handle = new IO::Socket::INET('PeerAddr' => $host,
						'PeerPort' => $port,
						'Proto'    => 'tcp');
	    if(not defined $comm_handle)
	      {
		print STDERR ( $UT,": COMM: Cannot open connection to ",
			       $connection,"!!\n" );
		print STDERR $UT,": COMM: ",$!,"\n";
		return undef;
	      }

	    print STDERR ( $UT,": COMM: Opened connection to ",
			   $host,":",$port,"\n");

	    $cached_handle=$comm_handle;
	    $SIG{"PIPE"}=\&reset_handle;
	  }

	# send the request

	my @SEND=();
	push @SEND,join(" ","GET",$remotefile,"HTTP/".$HTTPversion);
	push @SEND,"Host: ".$server_host if($HTTPversion eq "1.1");

	if(defined $user)
	  {
	    my $credentials=$user.":".$pass;

	    # Ugly Base64 encoding here, but since we can't rely on the
	    # MIME::Base64 package being installed, what else is possible
	    my $credencoded=pack('u',$credentials); 
	    $credencoded=substr($credencoded,1); 
	    $credencoded =~ tr# -_`#A-Za-z0-9+/=#;
	    $credencoded =~ s/[\n\r]*$//;  

	    push @SEND,"Authorization: Basic ".$credencoded;
	  }

	push @SEND,"";

	foreach ( @SEND )
	  {
	    print STDERR "******: COMM: SEND: ",$_,"\n" if ( $verbose );
	    $comm_handle->print($_,"\r\n");
	  }

	############################ Read response ############################
	
	my $line;

	$line=$comm_handle->getline;
	if(not defined $line)
	  {
	    print STDERR $UT,": COMM: Cannot read HTTP response!\n";
	    return undef;
	  }	    

	print STDERR $UT,": COMM: ",$line if ( $verbose );
	$line =~ s/[\n\r]*$//;

	my ($http,$code,$message)=split(/\s+/,$line,3);
	if($code != 200)
	  {
	    print STDERR ($UT,": COMM: HTTP response ",$code," message ",
			  $message,"\n");
	    return $code;
	  }	    

	$HTTPversion=substr($http,5);
	if ( $HTTPversion eq "1.0" )
	  {
	    undef $SIG{"PIPE"};
	    undef $cached_handle;
	  }

	my $content_length=undef;
	while(defined($line=$comm_handle->getline))
	  {
	    print STDERR $UT,": COMM: ",length($line)," ",$line if( $verbose );
	    $line =~ s/[\n\r]*$//;

	    last if($line eq "");

	    $line =~ s/\s*:\s*/:/;
	    my ($header,$value)=split(/:/,$line,2);
	    $header = lc $header;

	    $content_length=$value if($header eq "content-length");

	    if(($header eq "connection") && (lc $value eq "close"))
	      {
		undef $SIG{"PIPE"};
		undef $cached_handle;
	      }
	  }

	return new HTTPHandle($comm_handle,$content_length);
      }
  }

###############################################################################
################### GET THE SIZE OF THE FILE AND ITS CHECKSUM #################
###############################################################################

sub file_size_and_checksum;

sub test_file_equals
  {
    my $UT=shift;
    my $file=shift;

    my $r_size=shift;
    my $r_md5sum="AAA";

    print STDERR $UT,": COMPARE: file=",$file,"\n" if ( $verbose );

    if(not -e $file)
      {
	print STDERR ($UT,": COMPARE:  File not found\n") if ( $verbose );
	return 0;
      }
    
    my ($l_size,$l_md5sum)=file_size_and_checksum($UT,$file);

    print STDERR ($UT,": COMPARE:  Local ",sprintf("%-10d",$l_size),
		  $l_md5sum,"\n") if ($verbose);
    print STDERR ($UT,": COMPARE: Remote ",sprintf("%-10d",$r_size),
		  $r_md5sum,"\n") if ($verbose);

    return undef if (not defined $l_size);

    return 0 if( ($l_size != $r_size) || ($l_md5sum ne $r_md5sum) );

    return 1;
  }


sub file_size_and_checksum
  {
    my $utstring=shift;
    my $file=shift;

    my $sb=stat($file);
    if(not defined $sb)
      {
	print STDERR $utstring,": Cannot stat file: ",$file," ",$!,"\n";
	return undef;
      }
    my $bytes=$sb->size;
    
    my $fh=new FileHandle $file,"r";
    if(not defined $fh)
      {
	print STDERR $utstring,": Cannot open file: ",$file," ",$!,"\n";
	return undef;
      }

    my $md5digest="AAA";
    
    return $bytes,$md5digest;
  }

###############################################################################
############################### BUILD FILENAMES ###############################
###############################################################################

sub target_filename
  {
    my $UT=shift;
    my $filename=shift;
    my @cpts;
    push @cpts, "d".$UT if (defined $UT);
    push @cpts, $filename if (defined $filename);
    return join("/",$target_base_dir,@cpts);
  }

sub server_filename
  {
    my $UT=shift;
    my $filename=shift;
    my @cpts;
    push @cpts, "d".$UT if (defined $UT);
    push @cpts, $filename if (defined $filename);
    return join("/",$server_base_url,@cpts);
  }

###############################################################################
#################################### USAGE ####################################
###############################################################################

sub usage
  {
    print STDOUT 
      ("usage: $0 [-s] [-d directory] [UTDate]... [FileList]...\n\n",
       "Options:\n",
       "   -s: Do not download, only show how much space is needed\n",
       "   -d: Set target directory (place to put the downloaded files\n",
       "\n",
       "UTDate:   download all files from this date. Format is YYMMDD\n",
       "\n",
       "FileList: the file name of a list of runs to download. The list\n",
       "          must consist of lines in the format: YYMMDD RUNNO\n",
       "\n",
       "Examples: $0 000919\n",
       "             Download all runs from date 2000-09-19\n",
       "\n",
       "          $0 download.txt\n",
       "             Read the file download.txt and get all files listed\n",
       "             in it.\n",
      );
    exit;
  }

###############################################################################
############# WRAPPER AROUND THE SOCKET HANDLE WHICH UNDERSTANDS ##############
#############    HTTP/1.1 CHUNKING AND PERSISTANT CONNECTIONS    ##############
###############################################################################

package HTTPHandle;

# This package provides a useful interface to the (possibly) persistant 
# connections that arise with HTTP 1.0

sub new
  {
    my $class=shift;
    my $filehandle=shift;
    my $contentlength=shift;
    my $range=shift;

    my $self={ 
	      "handle"        => $filehandle,
	      "contentlength" => $contentlength,
	      "range"         => $range,
	      "chunk"         => "",
	      "chunkdone"     => 0,
	      "closed"        => 0,
	      "nread"         => 0,
	     };

    die "Cannot clone type ".bless($class) if ref($class);
    bless $self,$class;
    
  }

sub handle
  {
    my $self=shift;
    return $self->{"handle"};
  }

sub chunked
  {
    my $self=shift;
    return ( not defined $self->{"contentlength"} );
  }

sub bytesremaining
  {
    my $self=shift;
    return undef if $self->chunked;
    return ($self->{"contentlength"}-$self->{"nread"});
  }

sub eof
  {
    my $self=shift;
    
    return 1 if($self->{"closed"});
    if($self->chunked) { return $self->{"chunkdone"}; }
    else { return ( $self->bytesremaining==0 ); }
  }
    
sub read
  {
    my $self=shift;
    my $buffer=shift;
    my $maxsize=shift;
    
    if($self->eof)
      {
	$buffer="";
	return 0;
      }

    if($self->chunked)
      {
	my $nwritten=0;
	$$buffer="";

	while(($nwritten<$maxsize)&&(not $self->eof))
	  {
	    if(length $self->{"chunk"} > $maxsize-$nwritten)
	      {
		# Have more than sufficient bytes in chunk buffer
		$$buffer.=substr($self->{"chunk"},0,$maxsize-$nwritten);
		$self->{"chunk"}=substr($self->{"chunk"},$maxsize-$nwritten);
		return $maxsize;
	      }
	    
	    # Transfer full chunk buffer and refill
	    $$buffer .= $self->{"chunk"};
	    $nwritten += length($self->{"chunk"});
	    $self->{"chunk"} = 0;

	    my $line=$self->handle->getline;
	    print STDERR "******: COMM: ",length($line)," ",$line 
	      if( $verbose );

	    $line =~ /^([0-9a-fA-F]+)/;
	    my $chunksize = hex $1;

	    print STDERR "******: COMM: Chunksize ",$chunksize,"\n" 
	      if( $verbose );

	    if($chunksize != 0)
	      {
		my $chunkread=0;
		while($chunkread < $chunksize)
		  {
		    my $nr=
		      $self->handle->read($self->{"chunk"},
					  $chunksize-$chunkread,$chunkread);
		    $chunkread += $nr;
		    if($nr==0)
		      {
			print STDERR "******: COMM: Chunk error!!\n";
			return undef;
		      }
		  }
		$line=$self->handle->getline;
		print STDERR "******: COMM: ",length($line)," ",$line
		  if( $verbose );
	      }
	    else
	      {
		# Skip past trailer
		my $line;
		while(defined($line=$self->handle->getline))
		  {
		    print STDERR "******: COMM: ",length($line)," ",$line 
		      if( $verbose );
		    $line =~ s/[\n\r]*$//;
		    last if($line eq "");
		  }
		$self->{"chunkdone"}=1;
	      }
	  }
      }
    else
      {
	# not a chunked file, so just download it until we reach the end
	$maxsize = $self->bytesremaining 
	  if ( $maxsize > $self->bytesremaining );
	my $nr=$self->handle->read($$buffer,$maxsize);
	$self->{"closed"}=1 if ( $nr == 0 );
	$self->{"nread"}+=$nr;
	return $nr;
      }
  }

sub getline
  {
    my $self=shift;
    my $buffer="";

    my $c="";
    while(1)
      {
        if($self->read(\$c,1) == 0)
          {
            return undef  if (not $buffer);
            return $buffer;
          }
	
        $buffer.=$c;
        return $buffer if($c eq "\n");
      }
  }
