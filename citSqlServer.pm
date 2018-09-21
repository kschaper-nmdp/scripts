#!/usr/bin/perl
#
# package to connect to appropriate CIT SQL server/database
#
# $dbh = citConnect(e,s,d)
#	e - environment D|V|Q|I|P (D=dev, V=dev, Q=qa, I=int(external), P=production)
#	s - stack (0|1|2)
#	d - database (override for default database for given environment/stack)
#
# usage:
#	use citSqlServer;
#	my $dbh = citConnect('P',1);
#	# final parameter checks
#	if (($env ne 'D') && ($env ne 'Q') && ($env ne 'I') && ($env ne 'P')) {
#		printf "ERROR: only know about D|Q|I|P enviroment (-e)\n";
#		exit -1;
#	}
#
#	citConn($env) -- return connection string
#	citDB($env,$stack) -- return database name
#	citConnect($env, $stack, $specificDB) -- return DBI database connection
#
# databases associated with stacks
#	P - 0 - FormsNet2_PRD (default)
#		1 - FormsNet2
#		2 - FormsNet2
#	I - 0 - FormsNet3_FormDef
#		1 - FormsNet3_INT_AGNIS (default)
#		2 - FormsNet3_INT1_AGNIS
#	Q - 0 - FormsNet3_QA0
#		1 - FormsNet3_QA1
#		2 - FormsNet3_QA2 (default)
#	V - 1 - FormsNet3_DVL_TEST1
#		2 - FormsNet3_DVl_TEST2 (default)
#
# 2017-06-19 - ks - convert to new production 2014 server
# 2018-02-05 - ks - add doc on default values
# 2018-04-26 - ks - change V default to DVL_TEST
#

package citSqlServer;
use strict;
use warnings;
use Exporter;
use DBI;
#use DBD::ODBC;

use vars qw(@ISA @EXPORT);
our @ISA = qw(Exporter);
our @EXPORT = qw(citConnect citConn citDB citDebug);

our $citDebug;

sub citConn { my ($env) = @_;
	my $conn;
	if (!defined($env)) { return(undef); }
#	elsif ($env eq 'P') { $conn = "PSQLCLS2\\MS_LINK_PRD2"; }
	elsif ($env eq 'P') { $conn = "pcitsqlst2,27272"; }
	elsif ($env eq 'I') { $conn = "icitsqlst2,48431"; }
	elsif ($env eq 'Q') { $conn = "qcitsqlst2,47237"; }
	elsif ($env eq 'D') { $conn = "dcitsqlst2,61237"; }
	elsif ($env eq 'V') { $conn = "dcitsqlst2,61237"; }
	else {
		printf "ERR:citConn: Unknown env (%s)\n", $env;
		return(undef);
	}
	return $conn;
}

sub citDB { my ($env, $stack, $database) = @_;
	my $db;
	if (defined($database)) {
		$db = $database;
	} elsif (!defined($stack)) {
		if (!defined($env)) { return(undef); }
		elsif ($env eq 'P') { $db = 'FormsNet2_PRD'; }
		elsif ($env eq 'I') { $db = 'FormsNet3_INT_AGNIS'; }
		elsif ($env eq 'Q') { $db = 'FormsNet3_QA2'; }
		elsif ($env eq 'D') { $db = 'FormsNet3_DVL_TEST2'; }
		elsif ($env eq 'V') { $db = 'FormsNet3_DVL_TEST2'; }
	} else {
		if ($env eq 'P') {
			$db = 'FormsNet2'     if ($stack == 2);
			$db = 'FormsNet2'     if ($stack == 1);
			$db = 'FormsNet2_PRD' if ($stack == 0);
		} elsif ($env eq 'I') {
			$db = 'FormsNet3_INT1_AGNIS' if ($stack == 2);
			$db = 'FormsNet3_INT_AGNIS'  if ($stack == 1);
			$db = 'FormsNet3_FormDef'    if ($stack == 0);
		} elsif ($env eq 'Q') {
			$db = 'FormsNet3_QA2' if ($stack == 2);
			$db = 'FormsNet3_QA1' if ($stack == 1);
			$db = 'FormsNet3_QA0' if ($stack == 0);
		} elsif (($env eq 'D') || ($env eq 'V')) {
			$db = 'FormsNet3_DVL_TEST2' if ($stack == 2);
			$db = 'FormsNet3_DVL_TEST1' if ($stack == 1);
		}
	}
	return $db;
}

sub citConnect { my ($env, $stack, $specifiedDB) = @_;
	my $debug = $citDebug if (defined($citDebug));
	$debug = 0;
	printf "*D*:citConnect: env=%s, stack=%s, specifiedDB=%s \n",
		$env // '', $stack // '', $specifiedDB // '' if ($debug>0);

	my ($conn, $db);
	$conn = citConn($env);
	$db = citDB($env, $stack, $specifiedDB);
#	$db = $specifiedDB if defined($specifiedDB);
	printf "*D*:citConnect: conn=%s, db=%s\n", $conn,$db if ($debug>0);

	# initialize DB connection
	my $dbh = DBI->connect("DBI:ODBC:Driver={SQL Server};Server=$conn;Database=$db;Integrated Security=SSPI") 
		or die "Database connection not made: $DBI::errstr";
	$dbh->{RaiseError} = 1;
	return($dbh);
}
1
