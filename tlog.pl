#!/usr/bin/perl -w

use strict;

#
# Declare functions
#

sub usage;
sub expand_command;

sub main;

#
# Declare variables
#

my $LOGPATH             = '/home/observer';

my $SSH                 = '/usr/bin/ssh2';
my $SCP                 = '/usr/bin/scp2';

my @send_commands = 
  ( 
   q{$SSH observer@veritas.sao.arizona.edu mkdir /data/log/logs_$YEAR/d$YEARSHORT$MONTH/},
   q{$SCP $LOGFILE observer@veritas.sao.arizona.edu:/data/log/logs_$YEAR/d$YEARSHORT$MONTH/},

#   q{$SSH observer@egret.sao.arizona.edu mkdir /data/log/d$YEARSHORT$MONTH/},
#   q{$SCP $LOGFILE observer@egret.sao.arizona.edu:/data/log/d$YEARSHORT$MONTH/}, 

#   q{mkdir /vela/log/logs_$YEAR/d$YEARSHORT$MONTH},
#   q{cp $LOGFILE /vela/log/logs_$YEAR/d$YEARSHORT$MONTH}
 );

my @email_commands = 
  (
#   q{$SSH observer@veritas.sao.arizona.edu 'mail -s $LOGNAME sfegan@egret.sao.arizona.edu < /data/log/logs_$YEAR/d$YEARSHORT$MONTH/$LOGNAME'}
   );

main; 

sub main
  {
    #
    # Gather command line options
    #
    my $arg;
    my @UT;

    while(defined($arg=shift(@ARGV)))
      {
	if ( $arg =~ s/^-// )
	  {
	    # This arguement starts with a '-' so it is treated as an option.
	    while ( $arg =~ s/^(.)// )
	      {
		if ( $1 eq 'h' ) { usage; }
		print($0,": Unrecognised option: ",$1,"\n\n");
		usage;
	      }
	  }
	elsif ( $arg =~ /^d?\d{6,7}$/ ) 
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
	#my $time=time-1*60*60;
	#my @datecomponents=gmtime;
	#my $UT=sprintf("%2.2d%2.2d%2.2d",$datecomponents[5]%100,
	#	       $datecomponents[4]+1,$datecomponents[3]);
	#print("* -- No UT dates given, will transfer data for ",
	#      $UT,"\n\n");
	#push @UT,$UT

	print STDERR "No UT dates given... nothing to do\n\n";
	usage;
      }
    
    #
    # Loop through all the UT dates given.
    #
    
    my $UT;
    foreach $UT ( @UT )
      {
	my $command;
	foreach $command ( @send_commands, @email_commands )
	  {
	    my $run=expand_command($UT,$command);
	    print STDERR $UT,": ",$run,"\n";
	    system $run;
	  }
      }
  }

sub expand_command
  {
    my $utshort=shift;
    my $command=shift;

    my $logname="d".$utshort.".log_10";

    my $day=substr($utshort,-2,2);
    my $month=substr($utshort,-4,2);
    my $yearshort=substr($utshort,0,length($utshort)-4);
    my $year=$yearshort;
    $year += 1900 if(($year >= 80)&&($year < 1900));
    $year += 2000 if($year < 80);

    my $utlong=join('-',$year,$month,$day);
    
    my $logfile=join("/",$LOGPATH,$logname);
    
    $command =~ s/\$LOGNAME/$logname/ge;
    $command =~ s/\$LOGFILE/$logfile/ge;
    $command =~ s/\$YEARSHORT/$yearshort/ge;
    $command =~ s/\$YEAR/$year/ge;
    $command =~ s/\$MONTH/$month/ge;
    $command =~ s/\$DAY/$day/ge;
    $command =~ s/\$UTSHORT/$utshort/ge;
    $command =~ s/\$UTLONG/$utlong/ge;
    $command =~ s/\$SSH/$SSH/ge;
    $command =~ s/\$SCP/$SCP/ge;

    return $command;
  }

sub usage
  {
    print("usage: $0 [UT dates]\n");
    exit;
  }
