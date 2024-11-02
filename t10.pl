#!/usr/bin/perl -w

use strict;

#
# Declare functions
#

sub usage;
sub remote_file_same;
sub remote_copy_file;
sub remote_make_contents;

#
# Declare variables
#

my $SSH_destination_ac  = 'observer@veritas.sao.arizona.edu';
my $SSH_destination_dir = '/data/current/raw10';
my $source_dir          = '/draco/raw10';

my $SSH                 = '/usr/bin/ssh2';
my $SCP                 = '/usr/bin/scp2 -q';
my $compressor          = '/usr/bin/bzip2';
my $remotesize          = '/usr/bin/wc -c';
my $makecontents        = 'transfer_10/makecontents.pl';

my %preprocess_me       = ('fz'    => "$compressor FILE",
			   'fz.gz' => "/usr/bin/gzip -d FILE && $compressor ROOT");
my $transfer_me         = '*.fz *.fz.gz *.fz.bz2';

my @UT=();
my $UT=undef;
my $NoCompress=0;

my $arg;

#
# Gather command line options
#

while(defined($arg=shift(@ARGV)))
  {
    if ( $arg =~ s/^-// )
      {
	# This arguement starts with a '-' so it is treated as an option.
	while ( $arg =~ s/^(.)// )
	  {
	    if ( $1 eq 'n' ) { $NoCompress=1; next; }
	    if ( $1 eq 'h' ) { usage; }
	    print($0,": Unrecognised option: ",$1,"\n\n");
	    usage;
	  }
      }
    elsif ( $arg =~ /^\d{6,7}$/ ) 
      {
	# If the arguement is composed of digits only then presume that 
	# it is a UT date.
	push @UT,$arg;
      }
    else 
      { 
	print($0,": Arguement does not seem to be UT date: ",
	      $arg,"\n\n");
	usage;
      }
  }

#
# Figure out the current UT date if none is given
#

if ( scalar(@UT) == 0 )
  {
    # Take the current time and subtract 1 hour so that you
    # can transfer_10 last nights stuff until about 6pm
    # the following day (during winter)
    my $time=time-1*60*60;
    my @datecomponents=gmtime;
    my $UT=sprintf("%2.2d%2.2d%2.2d",$datecomponents[5]%100,
		   $datecomponents[4]+1,$datecomponents[3]);
    print("* -- No UT dates given, will transfer data for ",
	  $UT,"\n\n");
    push @UT,$UT
  }

#
# Tidy some variables up
#

$source_dir.="/" unless ( $source_dir=~/\/$/ );
$SSH_destination_dir.="/" unless 
  ( $SSH_destination_dir=~/\/$/ );

#
# Loop through all the UT dates given.
#

foreach $UT ( @UT )
  {
    my $Dir=$source_dir."d".$UT."/";
    
    # Make sure the source directory exists.
    
    if ( not -d $Dir )
      {
	print($UT,": No directory ",$Dir," .. skipping\n");
	next;
      }
    
    #  If we are not forbidden from compressing the data then
    #  find all of the relevent files and compress them
    
    if ( $NoCompress==0 )
      {
        my $GlobList=join(' ',map { "*.".$_ } keys %preprocess_me);
        my @FileList=
	  glob(join(" ",map({$Dir.$_} 
			    split(/\s+/,$GlobList))));
	
	if ( scalar @FileList )
	  {
	    my $file;
	    print($UT,": Found ",scalar(@FileList),
		  " file",((scalar(@FileList)==1)?"":"s"),
		  " to compress.\n");
	    
	    foreach $file ( @FileList )
	      {
		my $type = (grep { substr($file,-length($_)) eq $_ } 
			    keys %preprocess_me)[0];
		next unless ( exists $preprocess_me{$type} );
		my $action = $preprocess_me{$type};
		my $root=$file;
		$root =~ s/[.][^.]*$//;
		$action =~ s/FILE/$file/g;
		$action =~ s/ROOT/$root/g;
		print($UT,": Compressing ",$file,"\n");
		system($action);
	      }
	  }
	else
	  {
	    print($UT,": Found no files to compress.\n");
	  }
      }
    
    # Finally, transfer the data using scp. We check before the size of the 
    # file on the remote machine (if it exists) to see whether we should 
    # transfer or not, i.e. if its already there don't transfer it again.
    
    my @FileList=
      glob(join(" ",
		map({$Dir.$_} split(/\s+/,$transfer_me))));
    
    if ( scalar @FileList )
      {
	my $file;
	my $remote_dir=$SSH_destination_dir."d".$UT."/";
	my $SSH_dest=$SSH_destination_ac.":".$remote_dir;
	
	print($UT,": Found ",scalar(@FileList),
	      " file",((scalar(@FileList)==1)?"":"s"),
	      " to transfer.\n");
	
	# If it isn't there already then make the directory
	
	print($UT,": Creating directory ",$SSH_dest,".\n");
	system(join(" ",$SSH,$SSH_destination_ac,
		    "'","test -d",$remote_dir,"|| mkdir",
		    $remote_dir."'"));
	
	foreach $file ( @FileList )
	  {
	    my $goes=0;
	    while(not remote_file_same($file,$SSH_destination_dir."d".$UT."/"))
	      {
		print($UT,": File transfer failed, remote file differ.\n")
		  if ($goes);
		
		print($UT,": Transferring ",$file," -- ",
		      sprintf("%.1f",(-s $file,)/1024000),"M.b\n",);
		system(join(" ",$SCP,$file,$SSH_dest));
		$goes++;
		
		if($goes==10)
		  {
		    print($UT,": ",$goes," failed goes. Aborting...\n");
		    exit;
		  }
	      }
	    print($UT,": Not transferring ",$file,", remote file same.\n")
	      unless($goes);

	    # remote_copy_file($UT,$file,$remote_dir);
	  }
      }
    else
      {
	print($UT,": Found no files to transfer.\n");
      }

    print($UT,": Making contents file on remote machine.\n");
    remote_make_contents($UT);
  }

# Use SSH to connect to the remote machine and check the size of the file there
# (if it exists). Return 1 if remote file exists and its size is the same as
# the local size, 0 otherwise

sub remote_file_same
  {
    my $file=shift;
    my $remote_dir=shift;
    my $pathless_file=$file;
    my $line;
    
    $pathless_file =~ s/^.*\/([^\/]*)$/$1/;
    
    open FP,join(" ",$SSH,$SSH_destination_ac,      # Open a pipe to SSH and
		 "'"."test -e",	                    # execute 
		 $remote_dir.$pathless_file,        #   "test -e remotefile &&
		 "&&",$remotesize,                  #    du -b remotefile"
		 $remote_dir.$pathless_file."' |"); # on the remote machine
    $line=<FP>;
    
    close FP if $line;
    
    if($line)
      {
	$line=~s/^\s*(\d+)\s.*$/$1/;
	return 1 if($line == -s $file);
      }	
    
    return 0;
  }

sub usage
  {
    print("usage: $0 [-n] [UT dates]\n",
	  "\n",
	  "options:\n",
	  "  -n    Do not compress data before transfer\n");
    exit;
  }

sub remote_make_contents
  {
    my $UT=shift;
    system qq{$SSH $SSH_destination_ac $makecontents $UT};
  }    

# USE SSH to conenct to the remote machine and initiate a copy and
# recompression of the data to another directory

sub remote_copy_file
  {
    my $UT=shift;
    my $File=shift;
    my $RemDir=shift;
    my $RemoteCopyDir="/data/gzip/raw10";
    my $CopyDir=$RemoteCopyDir."/d".$UT;
    
    print($UT,": Copying file on remote machine to ",$CopyDir,"\n");
    
    open FP,join(" ","|",$SSH,$SSH_destination_ac,"tcsh");
    
    #
    # Below is a little script to copy over the files where appropriate. 
    # It only copies if it hasn't already done so. It then uncompresses 
    # the file if it is a .bz2 and recompresses it as a .gz
    #
    
    print FP <<"END_SCRIPT_END";
set RemFile = $File
set FileName = \$RemFile:t
set OrigFile = $RemDir/\$RemFile:t
set CopyFile = $CopyDir/\$RemFile:t

if( ! -d $CopyDir ) mkdir $CopyDir
if(( ! -e \$CopyFile ) && ( ! -e \$CopyFile:r.gz )) then
  cp -p \$OrigFile \$CopyFile
  if ( \$CopyFile == \$CopyFile:r.bz2 ) then
    bunzip2 \$CopyFile
    gzip \$CopyFile:r
  endif
endif
END_SCRIPT_END
    
    close FP;
  }

# Recompresses all files in /draco/quicklook using bzip2 instead of gzip
# SD 990609

#system '/usr/users/observer/transfer_10/rezip &';
