#!/usr/bin/perl
#
# prototype to:
#	1. extract Patient from FHIR server
#	2. translate to FormsNet CRID request
#	3. submit to CRID request service
#		a. search for matching existing CRIDs
#		b. if 1 perfect match, return it
#		c. if multiple perfect matches, return error
#		d. if fuzzy match(s), save matching CRIDs and request new CRID
#		e. if no match, request new CRID
#	4. report result including any fuzzy matches
#
# curl -X GET http://cibmtr-hapi-prod.us-east-1.elasticbeanstalk.com/r3/Patient/2352
# curl -X GET http://fhirtest.b12x.org/baseDstu3/Patient/2481
#
# TBD
#
#

use strict;
use warnings;
use Getopt::Std;
use REST::Client;
use JSON;
use Data::Dumper;
use citFhirUtility;
use citCridService;

my $ccn;
my $env = 'D';
my $stack;
#my $fhirServer = 'http://cibmtr-hapi-prod.us-east-1.elasticbeanstalk.com/r3';
my $fhirServer = 'http://fhirtest.b12x.org/baseDstu3';
my $release = 3;
my $query = 0;
my $forceUpdate = 0;
my $debug = 0;
my %options = ();
getopts('c:e:s:S:r:qfd:h', \%options);
$ccn = $options{c} if defined($options{c});
$env = $options{e} if defined($options{e});
$stack = $options{s} if defined($options{s});
$fhirServer = $options{S} if defined($options{S});
$release = $options{r} if defined($options{r});
$query = 1 if defined($options{q});
$forceUpdate = 1 if defined($options{f});
$debug = $options{d} if defined($options{d});
my @ptIds = @ARGV;

if (defined($options{h})) {
	print "cridRequestor -c 10000 -e X -s 9 -S url -q -T -h -d 0 id
	-c 10000 -- CCN (required)
	-e X -- FormsNet DB environment, P|U|Q|D (default is D)
	-s 9 -- FormsNet DB stack, 0-2 (default is environment specific)
	-S url -- FHIR server to use (default is http://fhirtest.b12x.org/baseDstu3)
	-r n -- server is release n (default is 3)
	-q -- query FHIR server
	-f -- force update of CRID on server to potentially different value (default is ERROR)
	-h -- print this help
	-d 0 -- debug output (default is 0)
	id -- id(s) of Patient Resource(s) to request CRIDs for
";
	exit(0);
}

# finalize parameters
#$citCridService::debug = $debug;
#$citFhirUtility::debug = $debug;
if (($env ne 'D') && ($env ne 'V') && ($env ne 'Q') && ($env ne 'I') && ($env ne 'P')) {
	printf "ERROR: only know about D|Q|I|P environment (-e)\n";
	exit -1;
}
if (!defined($stack)) {
	$stack = 2 if (($env eq 'D') || ($env eq 'V'));
	$stack = 2 if ($env eq 'Q');
	$stack = 0 if ($env eq 'I');
	$stack = 0 if ($env eq 'P');
}
if ($stack != 0 && $stack != 1 && $stack != 2) {
	printf "ERROR: can only specify stack 0, 1, or 2\n";
	exit -1;
}
printf "CCN=%s, env=%s, stack=%s, fhirServer=%s\n", $ccn//'', $env, $stack, $fhirServer if ($debug>-1);
my $client = REST::Client->new();
my $urlFull = $fhirServer . '/Patient/';

# deal with queries separately
if ($query) {
	print "*D* urlFull=$urlFull\n" if ($debug>0);
	$client->GET($urlFull);
	my $jsonBundle = $client->responseContent();
	my $bundle = decode_json($jsonBundle);
	printf "*D* resourceType=%s, id=%s, total=%s\n", $$bundle{resourceType}, $$bundle{id}, $$bundle{total} if ($debug>0);
	my $entry = $$bundle{entry};
	my @entry = @$entry;
	my $nEntry = $#entry + 1;
	printf "*D* nEntry=%s\n", $nEntry if ($debug>0);
	printf "%10s %8s %6s %3s %10s %-30s\n", "resource", "id", "gender", "sex", "birthDate", "bestName";
	foreach my $e (@$entry) {
		my $resource = $$e{resource};
		print Dumper($resource) if ($debug>8);
		my $resourceType = $$resource{resourceType};
		my $id = $$resource{id};
		my ($given,$family,$birthDate,$gender,$birthSex,$allNames);
		if ($resourceType eq 'Patient') {
			my $patientInfo = parsePatient($resource);
			$given = $$patientInfo{given} // '';
			$family = $$patientInfo{family} // '';
			$gender = $$patientInfo{gender} // '';
			$birthSex = $$patientInfo{birthSex} // '';
			$birthDate = $$patientInfo{birthDate} // '';
			$allNames = $$patientInfo{allNames} // '';
		}
		printf "%10s %8s %6s %3s %10s %-30s    -- %s\n", $resourceType, $id // '', $gender, $birthSex, $birthDate, 
			$family.', '.$given, $allNames;
	}
	exit(0);
} elsif (! defined($ccn)) {
	print "ERROR: must define CCN for requesting CRID\n";
	exit(-1);
}

# get Patient resource content
my $patient;
my $crid;
foreach my $ptId (@ptIds) {

print "*D* getting patient $ptId from server $fhirServer\n" if ($debug>2);
my $client = REST::Client->new();
my $urlFull = $fhirServer . '/Patient/' . $ptId;
print "*D* urlFull=$urlFull\n" if ($debug>0);
$client->GET($urlFull);
my $json = $client->responseContent();
$patient = decode_json($json);
print Dumper($patient) if ($debug>3);

# make sure we have a Patient resource
my $resourceType = $$patient{resourceType};
if (!defined($resourceType) || ($resourceType ne 'Patient')) {
	print "ERROR: non-Patient resource \n";
	exit(-1);
}

# map Patient resource to CRID service input
my $patientInfo = parsePatient($patient);
# normalize gender to birthSex, then gender
$$patientInfo{gender} = $$patientInfo{birthSex} if defined($$patientInfo{birthSex});
printf "given=%s, family=%s, gender=%s, dob=%s\n", 
	$$patientInfo{given} // '', $$patientInfo{family} // '', 
	$$patientInfo{gender} // '', $$patientInfo{birthDate} // '';

# search for matches to existing CRIDs
my $matches = cridSearch($env, $stack, $patientInfo);
print Dumper($matches) if ($debug>8);

# deal with match results
if ($$matches{nPM} > 1) {
	print "Multiple perfect matches\n";
	my $PMarr = $$matches{PM};
	my %unique;
	foreach my $pMatch (@$PMarr) {
		$unique{$$pMatch[1]} = 1;
	}
	printf "Perfect matches: %s\n", join(' ', (keys %unique));
	next;
} elsif ($$matches{nPM} == 1) {
	my $PM = $$matches{PM};
	my $PM1 = $$PM[0];
	$crid = $$PM1[1];
	printf "Single perfect match (%s)\n", $crid;
} elsif ($$matches{nFM} > 0) {
	print "creating new CRID with some fuzzy matches\n";
	my $FMarr = $$matches{FM};
	my %unique;
	foreach my $fMatch (@$FMarr) {
		$unique{$$fMatch[1]} = 1;
	}
	printf "Fuzzy matches: %s\n", join(' ', (keys %unique));
	$crid = assignCrid($env, $stack, $ccn, $patientInfo);
	printf "new CRID=%s\n", $crid;
} else {
	print "creating new CRID\n";
	$crid = assignCrid($env, $stack, $ccn, $patientInfo);
	printf "new CRID=%s\n", $crid;
}	

# store CRID back on server (PUT)
#
# identifier -> array of hashes
#	each hash
#		'use' (code -- official)
#		'system' (URI, namespace for identifer value)
#		'value' (string)
my %ident = (
	use => 'official',
	system => 'http://cibmtr.org/cibmtr_research_identifier.txt',
	value => "$crid"
);
#
# see if CRID is already part of this Resource
my $identifier = $$patient{identifier};
my $first = 1;
my $foundCridIdentifier = 0;
my $cridAlreadyPresent = 0;
my $differentCridFound = 0;
my $differentCrid;
my @identifier;
if (defined($identifier)) {
	foreach my $id (@$identifier) {
		printf "%8s %-30s %s\n", "use","system","value" if ($first && $debug>0);
		$first = 0;
		printf "%8s %-30s %s\n", $$id{use} // 'undef', $$id{system} // 'undef', $$id{value} // 'undef' if ($debug>0);
		if (($$id{use} eq 'official') && ($$id{system} eq 'http://cibmtr.org/cibmtr_research_identifier.txt')) {
			$foundCridIdentifier = 1;
			if ($$id{value} eq "$crid") {
				$cridAlreadyPresent = 1;
			} else {
				$differentCridFound = 1;
				$differentCrid = $$id{value};
				if ($forceUpdate) {
					printf "*D* forcing new CRID value, %s to %s\n", $differentCrid, $crid if ($debug>0);
					$$id{value} = $crid;
				}
			}
		}
	}
	printf "*D* foundCridIdentifier=%s, cridAlreadyPresent=%s, differentCridFound=%s\n", 
		$foundCridIdentifier, $cridAlreadyPresent, $differentCridFound if ($debug>0);
	#
	# if not, create a new Identifier instance
	if ($differentCridFound && !$forceUpdate) {
		printf "ERROR: resource associated with different CRID (%s)\n", $differentCrid;
		next;
	}
	if ($cridAlreadyPresent) {
		printf "INFO: resource already associated with CRID (%s)\n", $crid;
		next;
	}	
	@identifier = @$identifier;
}
# add to existing Identifiers
if (! $foundCridIdentifier) {
	print "*D* adding CRID identifier\n" if ($debug>0);
	push(@identifier,\%ident);
	$$patient{identifier} = \@identifier;
}
print Dumper($patient) if ($debug>3);

# encode to JSON
$json = encode_json($patient);

# PUT resource on server
printf "*D* putting resource back to %s\n", $urlFull if ($debug>0);
$client->PUT($urlFull, $json) or warn "error PUTting resource onto server\n";

}  # end foreach ptId
