#!/usr/bin/perl
#
# package with various routines to interact with FHIR resources
#
# $dbh = citConnect(e,s,d)
#	e - environment D|V|Q|I|P (D=dev, V=dev, Q=qa, I=int(external), P=production)
#	s - stack (0|1|2)
#	d - database (override for default database for given environment/stack)
#
# usage:
#	use citFhirUtility;
#	$patientInfo = parsePatient($patientResourceReference);
#		$patientInfo -- hash reference
#			given, family, birthSex, birthDate, gender, allNames
#
# 2018-04-26 - ks - original
# 2018-04-17 - ks - separate gender from birthSex
# 2018-04-17 - kas - prioritize name to official, then usual, then first
# 2018-05-03 - kas - change family name parsing to accomodate r2 arrays
# 2018-07-26 - kas - add identifiers
# 2018-09-04 - kas - convert to REST service
#

package citFhirUtility;
use strict;
use warnings;
use Exporter;
use citSqlServer;

#use vars qw(@ISA @EXPORT);
use Exporter;
our @ISA = 'Exporter';
our @EXPORT = qw(parsePatient $fhirDebug);

our $fhirDebug = 0;

#sub fhirDebug {
#	my ($dbg) = $_;
#	$debug = $dbg;
#}

##################################################################################
# parse patient resource
sub parsePatient {
	my ($patient) = @_;
	my %resp;

	#-----
	# name -- choose first official or first usual use or first
	my $name = $$patient{name};
	if (!defined($name)) {
		print "Warning: missing name\n" if ($fhirDebug>0);
	} else {
		# find "best" name instance -- first "official" use, else first "usual" use, else first
		my $best;
		printf "*D* parsePatient: looking for 'official' use\n" if ($fhirDebug>3);
		foreach my $nameInstance (@$name) {
			my $thisUse = $$nameInstance{use};
			if (defined($thisUse) && ($thisUse eq 'official')) {
				printf "*D* parsePatient: found 'official' use\n" if ($fhirDebug>3);
				$best = $nameInstance;
				last;
			}
		}
		if (!defined($best)) {
		printf "*D* parsePatient: looking for 'usual' use\n" if ($fhirDebug>3);
		foreach my $nameInstance (@$name) {
			my $thisUse = $$nameInstance{use};
			if (defined($thisUse) && ($thisUse eq 'usual')) {
				printf "*D* parsePatient: found 'usual' use\n" if ($fhirDebug>3);
				$best = $nameInstance;
				last;
			}
		}
		}
		if (!defined($best)) {
			printf "*D* parsePatient: choosing first name instance\n" if ($fhirDebug>3);
			$best = $$name[0];
		}
		my $family = $$best{family};
		if (ref($family) eq 'ARRAY') {	# check for R2 format
			$family = join(' ', @$family) if (defined($family));
		}
		$resp{lastName} = $family // '';
		my $given = $$best{given};
		$resp{firstName} = (defined($given)) ? join(' ',@$given) : '';
	}

	#-----
	# allNames -- all recorded names
	my $allNames = '';
	if (defined($name)) {
		# concatenate all name instances
		foreach my $nameInstance (@$name) {
			my $use = $$nameInstance{use} // '';
			my $family = $$nameInstance{family};
			if (ref($family) eq 'ARRAY') {
				$family = join(' ', @$family) if (defined($family));
			}
			$family = '' if (!defined($family));
			my $givenArray = $$nameInstance{given};
			my $given = (defined($givenArray)) ? join(' ',@$givenArray) : '';
			$allNames .= "; " if (length($allNames) > 0);
			$allNames .= "$family, $given ($use)";
		}
	}
	$resp{allNames} = $allNames;
	
	#-----
	# birthday
	$resp{birthDate} = $$patient{birthDate};

	#-----
	# gender
	my $gender = $$patient{gender};
	# normalize gender
	$gender = 'M' if (defined($gender) && (lc($gender) eq 'male'));
	$gender = 'F' if (defined($gender) && (lc($gender) eq 'female'));
	
	$resp{gender} = $gender;

	#-----
	# extensions
	my ($birthSex, $mothersMaidenName, $birthCountry, $birthState, $birthCity);
	my $extension = $$patient{extension};
	if (defined($extension)) {
		my @extension = @$extension;
		my $nExtension = $#extension;
		print "*D* parsePatient: nExtension=$nExtension\n" if ($fhirDebug>3);
		for (my $i=0; $i<=$nExtension; $i++) {
			my $item = $extension[$i];
			my $url = $$item{url};
			if (! defined($url)) {
				print "Warning: no URL defined for extension %d\n", $i if ($fhirDebug>0);
				next;
			} elsif ($url eq 'http://hl7.org/fhir/StructureDefinition/us-core-birth-sex') {
				my $concept = $$item{valueCodeableConcept};
				my $coding = $$concept{coding};
				my $first = $$coding[0];
				my $system = $$first{system};
				if ($system ne 'http://hl7.org/fhir/v3/AdministrativeGender') {
					printf "Warning: wrong system (%s)\n", $system if ($fhirDebug>0);
					next;
				}
				$resp{birthSex} = $$first{code};
			} elsif ($url eq 'http://hl7.org/fhir/StructureDefinition/patient-birthPlace') {
				# birthplace, type Address
				my %address = %$item{birthPlace};
				$resp{birthCountry} = $address{country};		# TBD: may be ISO 3166 2 or 3 letter code
				$resp{birthState} = $address{state};
				$resp{birthCity} = $address{city};
			} elsif ($url eq 'http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName') {
				$resp{mothersMaidenName} = $$item{mothersMaidenName};
			} else {
				printf "*D* parsePatient: unknown extension (%s)\n", $url if ($fhirDebug>0);
			}
		}
	} # end if if extensions

	#-----
	# identifiers
	my $identifier = $$patient{identifier};
	if (defined($identifier)) {
		my @identArray = @$identifier;
		my @allIdent = ();
		foreach my $id (@identArray) {
			my %hash = %$id;
			if (!defined($hash{system})) {
				printf "ERR: no identifier.system\n";
				next;
			} elsif ($hash{system} eq 'http://hl7.org/fhir/sid/us-ssn') {
				$resp{ssn} = $hash{value};
			} elsif ($hash{system} eq 'http://cibmtr.org/fhir/crid') {
				$resp{crid} = $hash{value};
			} elsif ($hash{system} eq 'http://nmdp.org/fhir/rid') {
				$resp{nmdpRid} = $hash{value};
			} elsif ($hash{system} =~ /http:\/\/ebmt\.org\/fhir\/uid\/(\d+)/) {
				$resp{ebmtCic} = $1;
				$resp{ebmtId} = $hash{value};
			} elsif ($hash{system} =~ /http:\/\/cibmtr\.org\/cibmtr\/fhir\/(\d+)/) {
				$resp{cibmtrTeam} = $1;
				$resp{cibmtrIubmid} = $hash{value};
			} elsif ($hash{system} eq 'http://usidnet.org/fhir/upn') {
				$resp{usidnetId} = $hash{value};
			} elsif ($hash{system} eq 'http://apbmt.org/fhir/upn') {
				$resp{apbmtId} = $hash{value};
			} elsif ($hash{system} eq 'http://cbmtg.org/fhir/upn') {
				$resp{cbmtgId} = $hash{value};
			} elsif ($hash{system} eq 'http://embmt.org/fhir/upn') {
				$resp{embmtId} = $hash{value};
			} else {
				printf "*D* parsePatient: unknown identifier.system (%s)\n", $hash{system} if ($fhirDebug>0);
			}
		}
	}

	return(\%resp);
}

1
