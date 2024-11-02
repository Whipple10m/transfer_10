#!/usr/bin/perl -w

use strict;

use File::stat;
use FileHandle;
use MD5;

#
# Declare functions
#

sub file_size_and_checksum;

#
# Declare constants
#

my $contents_file      = 'contents';
my $source_dir         = '/data/current/raw10';
my $process_me         = '*.fz *.fz.gz *.fz.bz2';

#
# Declare variables
#

my @UT=();
my $UT=undef;

my $arg;

#
# Gather command line options
#

while(defined($arg=shift(@ARGV)))
{
  if ( $arg =~ /^\d{6,7}$/ ) 
    {
      # If the arguement is composed of digits only then
      # presume that it is a UT date.
      push @UT,$arg;
    }
  elsif ( $arg =~ /^d(\d{6,7})$/ ) 
    {
      # If the arguement is composed of something like d000101 then
      # presume that it is a UT date.
      push @UT,$1;
    }
  else 
    { 
      print($0,": Arguement does not seem to be UT date: ",
	    $arg,"\n\n");
    }
}

#
# Figure out the current UT date if none is given
#

if ( scalar(@UT) == 0 )
{
#   Take the current time and subtract 1 hour so that you
#   can transfer_10 last nights stuff until about 6pm
#   the following day (during winter)
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

#
# Loop through all the UT dates given.
#

foreach $UT ( @UT )
{
    my $Dir=$source_dir."d".$UT."/";

#   Make sure the source directory exists.
   
    if ( not -d $Dir )
    {
	print($UT,": No directory ",$Dir," .. skipping\n");
	next;
    }

    my $cfilename=$Dir.$contents_file;
    my $cfile=new FileHandle $cfilename,"w";
    if(not defined $cfile)
      {
	print($UT,": Could not open contents file ",$cfilename,
	      " .. skipping\n");
	next;
      }

    $cfile->print("######## START VERITAS DATA CONTENTS FILE $UT ########\n");

    my @FileList=
      glob(join(" ",map({$Dir.$_} split(/\s+/,$process_me))));
    
    if ( scalar @FileList )
    {
	print($UT,": Found ",scalar(@FileList),
	      " file",((scalar(@FileList)==1)?"":"s"),
	      " to list.\n");
	
	my $filepath;
	foreach $filepath ( @FileList )
	{
	  my $file=(split("/",$filepath))[-1];

	  my ($size,$checksum)=file_size_and_checksum($UT,$filepath);
	  next if(not defined $size);
	  
	  print($UT,
		": File ",$file,
		" Size ",sprintf("%-8d",$size),
		" MD5 ",$checksum,"\n");
	  
	  $cfile->print($file," ",sprintf("%-8d",$size)," ",$checksum,"\n");
	}
      }
    else
      {
	print($UT,": Found no files to transfer.\n");
      }

    $cfile->print("######### END VERITAS DATA CONTENTS FILE $UT #########\n");
  }

sub file_size_and_checksum
  {
    my $utstring=shift;
    my $file=shift;

    my $sb=stat($file);
    if(not defined $sb)
      {
	print $utstring,": Cannot stat file: ",$file," ",$!,"\n";
	return undef;
      }
    my $bytes=$sb->size;
    
    my $fh=new FileHandle $file,"r";
    if(not defined $fh)
      {
	print $utstring,": Cannot open file: ",$file," ",$!,"\n";
	return undef;
      }

    my $MD5=new MD5;
    $MD5->addfile($fh);
    $fh->close;
	    
    my $md5digest=$MD5->hexdigest;
    undef $MD5;
    
    return $bytes,$md5digest;
  }
    
