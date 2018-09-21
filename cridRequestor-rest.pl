#!/usr/bin/perl
#
# prototype to:
#	1. extract Patient from FHIR server
#	2. translate to FormsNet CRID request
#	3. submit to CRID request service
#	4. Patient resource on FHIR server with new identifier
#
# Patient attributes
#	(required for matching)
#		firstName
#		lastName
#		birthDate
#		gender -- if US-Core.birthSex is present, use it, else use Patient.gender 
#	(optional for matching)
#		ssn -- Patient.identifier.type=SB,URI=http://hl7.org/fhir/sid/us-ssn
#		nmdpRid -- Patient.identifier.type=REG,URI=http://nmdp.org/fhir/rid
#		mothersMaidenName -- extension Patient-mothersMaidenname
#		ebmtCic/ebmtId
#			id - Patient.identifier.type=REG,URI=http://ebmt.org/fhir/CIBMTR/<cic>
#		cibmtrTeam/cibmtrIubmid
#			iubmid - Patient.identifier.type=REG,URI=http://cibmtr.org/fhir/iubmid/<team>
#	(archiving only)
#		birthCity -- extension Patient-birthPlace
#		birthState	 -- extension Patient-birthPlace
#		birthCountry -- extension Patient-birthPlace
#		usidnetId -- Patient.identifier.type=REG,URI=http://usidnet.org/fhir/upn
#		apbmtId -- Patient.identifier.type=REG,URI=http://apbmt.org/fhir/upn
#		cbmtgId -- Patient.identifier.type=REG,URI=http://cbmtg.org/fhir/upn
#		embmtId -- Patient.identifier.type=REG,URI=http://embmt.org/fhir/upn
#		mdsStudyId -- Patient.identifier.type=REG,URI=http://cibmtr.org/mdsStudy
#		other registry/id
#			id - Patient.identifier.type=REG,URI=http://cibmtr.org/fhir/<REGISTRY>
#
# registry summary
#	nmdp.org/fhir -- rid
#	cibmtr.org/fhir -- crid, team, iubmid, 
#	ebmt.org/fhir -- cic, id
#
#
# TODO
# server-specific timestamp file
#
#
# 2018-05-15 - kas - original
# 2018-07-10 - kas - convert to FN REST calls
# 2018-09-10 - kas - correct PUT call with bodyContent and header information
# 2018-09-10 - kas - add (for debugging) explicit given/family/dob/sex
# 2018-09-10 - kas - add identifier update on FHIR server
# 2018-09-13 - kas - add logic to report patient records updated since last invocation
# 2018-09-18 - kas - add -x to reset CRIDS on fhir server
# 2018-09-19 - kas - add -u to control resetting of timestamp
#

use strict;
use warnings;
use citFhirUtility;
use citCridService;
use Getopt::Std;
use JSON;
use REST::Client;
use Data::Dumper;

# constants
my $CridUri = 'http://cibmtr.org/fhir/crid';
my $timeStampFile = 'u:/Ideas/FHIR/FhirSubmission/timestamp.txt';

# parse parameters
my $ccn;
my $env = 'D';
my $stack;
my $fhirServer = 'http://fhirtest.b12x.org/r2';
my $cridRestService = 'http://10.99.0.246:8089/CRID';
my $query = 0;
my $update = 0;
my $reset = 0;
my $forceUpdate = 0;
my ($debugGiven, $debugFamily, $debugDoB, $debugGender);
my $debug = 0;
my %options = ();
getopts('xquc:e:s:S:R:G:F:D:H:fd:h', \%options);
$ccn = $options{c} if defined($options{c});
$env = $options{e} if defined($options{e});
$stack = $options{s} if defined($options{s});
$fhirServer = $options{S} if defined($options{S});
$cridRestService = $options{R} if defined($options{R});
$query = 1 if defined($options{q});
$update = 1 if defined($options{u});
$reset = 1 if defined($options{x});
$debugGiven = $options{G} if defined($options{G});
$debugFamily = $options{F} if defined($options{F});
$debugDoB = $options{D} if defined($options{D});
$debugGender = $options{H} if defined($options{S});
$forceUpdate = 1 if defined($options{f});
$debug = $options{d} if defined($options{d});
my @ptId = @ARGV;

$citFhirUtility::fhirDebug = 1 if ($debug>1);

if (defined($options{h})) {
	print "cridRequestor -c 10000 -e X -s 9 -S url -q -T -h -d 0 id
	-c 10000 -- CCN (required)
	-e X -- FormsNet DB environment, P|U|Q|D (default is D)
	-s 9 -- FormsNet DB stack, 0-2 (default is environment specific)
	-S url -- FHIR server to use (default is $fhirServer)
	-R url -- REST service to use (default is $cridRestService)
	-q -- only query FHIR server
	-u -- for query, update timestamp
	-x -- reset existing CRIDs on FHIR server
	-G given -- given name (debug purposes)
	-F family -- family name (debug purposes)
	-D dob -- dateOfBirth (debug purposes)
	-H gender -- gender (debug purposes)
	-f -- force update of CRID on server to potentially different value (default is ERROR)
	-h -- print this help
	-d 0 -- debug output (default is 0)
	id -- id(s) of Patient Resource(s) to request CRIDs for
";
	exit(0);
}

# finalize parameters
if (($env ne 'D') && ($env ne 'V') && ($env ne 'Q') && ($env ne 'I') && ($env ne 'P')) {
	printf "ERROR: only know about D|V|Q|I|P environment (-e)\n";
	exit -1;
}
if (!defined($stack)) {
	$stack = 2 if (($env eq 'D') || ($env eq 'V'));
	$stack = 2 if ($env eq 'Q');
	$stack = 1 if ($env eq 'I');
	$stack = 0 if ($env eq 'P');
}
if ($stack != 0 && $stack != 1 && $stack != 2) {
	printf "ERROR: can only specify stack 0, 1, or 2\n";
	exit -1;
}
if (!defined($cridRestService)) {
	printf "ERROR: must specify REST server\n";
	exit(-1);
}
printf "INFO: CCN=%s, env=%s, stack=%s, fhirServer=%s, restService=%s\n", $ccn//'', $env, $stack, $fhirServer, $cridRestService if ($debug>-1);
my $fhirClient = REST::Client->new();
my $fhirPatientUrl;

# deal with reset
if ($reset) {
	foreach my $ptId (@ptId) {
		# get the Patient resource
		$fhirPatientUrl = $fhirServer . '/Patient/' . $ptId;
		$fhirClient->GET($fhirPatientUrl);
		my $patientJson = $fhirClient->responseContent();
		my $patient = decode_json($patientJson);

		# if present, remove the CRID identifier
		my $identifier = $$patient{identifier};
		my (@updatedIdentifier, $foundCrid, $existingCrid);
		$foundCrid = 0;
		if (defined($identifier)) {
		 	foreach my $id (@$identifier) {
				if ($$id{system} eq $CridUri) {
					$foundCrid = 1;
					$existingCrid = $$id{value};
				} else {
					push(@updatedIdentifier, $id);
				}
			}
			if ($foundCrid) {
				printf "*D* Patient %s: found and removed existing CRID identifier (%s)\n", $ptId, $existingCrid if ($debug>0);
				$$patient{identifier} = \@updatedIdentifier;
			} else {
				printf "*D* no CRID found for patient %s\n", $ptId if ($debug>0);
			}
		}

		# if removed CRID, update Patient resource, encode to JSON && PUT resource on server
		if ($foundCrid) {
			$patientJson = encode_json($patient);
			printf "*D* putting resource back to %s\n", $fhirPatientUrl if ($debug>0);
			$fhirClient->PUT($fhirPatientUrl, $patientJson) or warn "error PUTting resource onto server\n";
		}
	}
	exit(0);
}

# get last timestamp
open FD, "<$timeStampFile" or die "error opening timestamp file ($timeStampFile): $!";
my $timeStamp = <FD>;
chomp($timeStamp);
printf "*D* timeStamp = %s\n", $timeStamp if ($debug>0);
my ($luYear, $luMonth, $luDay, $luHour, $luMinute, $luSecond, $luMicro, $luTz);
#                    1:year     2:mo    3:day  4:hr    5:min   6:sec   7:usec    8:tz
if ($timeStamp =~ /(\d\d\d\d)\-(\d\d)\-(\d\d)T(\d\d)\:(\d\d)\:(\d\d)\.(\d\d\d)\+(\d\d\:\d\d)/) {
	$luYear = $1;  $luMonth = $2;  $luDay = $3;  $luHour = $4;  $luMinute = $5;  $luSecond = $6;  $luMicro = $7;  $luTz = $8;
} else {
	printf "Error parsing timestampFile\n";
}
close FD;
my $timeStampStr = sprintf("%04d-%02d-%02dT%02d:%02d:%02d.%03d", $luYear,$luMonth,$luDay, $luHour,$luMinute,$luSecond,$luMicro);
my $timeStampMax = $timeStampStr;

# loop through all patients, looking for new records
my @newId;
$fhirPatientUrl = $fhirServer . '/Patient?_lastUpdated=gt' . sprintf("%04d-%02d-%02dT%02d:%02d:%02d", $luYear,$luMonth,$luDay, $luHour,$luMinute,$luSecond);
print "*D* fhirPatientUrl=$fhirPatientUrl\n" if ($debug>0);
$fhirClient->GET($fhirPatientUrl);
my $jsonBundle = $fhirClient->responseContent();
my $bundle = decode_json($jsonBundle);
printf "*D* resourceType=%s, id=%s, total=%s\n", $$bundle{resourceType}, $$bundle{id}, $$bundle{total} if ($debug>1);
my $entry = $$bundle{entry};
my @entry;
@entry = @$entry if defined($entry);
my $nEntry = $#entry + 1;
printf "*D* nEntry=%s\n", $nEntry if ($debug>1);
foreach my $e (@$entry) {
	my $resource = $$e{resource};
	print Dumper($resource) if ($debug>8);
	my $resourceType = $$resource{resourceType};
	my $id = $$resource{id};
 	my $meta = $$resource{meta};
	if (defined($meta)) {
		my %hash = %$meta;
		my $lastUpdated = $hash{lastUpdated};
		my ($luYear2, $luMonth2, $luDay2, $luHour2, $luMinute2, $luSecond2, $luMicro2, $luTz2);
		if (defined($lastUpdated) && ($lastUpdated =~ /(\d\d\d\d)\-(\d\d)\-(\d\d)T(\d\d)\:(\d\d)\:(\d\d)\.(\d\d\d)\+(\d\d\:\d\d)/)) {
			$luYear2 = $1;  $luMonth2 = $2;  $luDay2 = $3;  $luHour2 = $4;  $luMinute2 = $5;  $luSecond2 = $6;  $luMicro2 = $7;  $luTz2 = $8;
			my $timeStampStr2 = sprintf("%04d-%02d-%02dT%02d:%02d:%02d.%03d", $luYear2,$luMonth2,$luDay2, $luHour2,$luMinute2,$luSecond2,$luMicro2);
			printf "*D* id=%s, lastUpdated=%s\n", $id, $timeStampStr2 if ($debug>0);
			if ($timeStampStr2 gt $timeStampMax) {
				$timeStampMax = $timeStampStr2;
				printf "*D* new maxTime = %s\n", $timeStampMax if ($debug>2);
				$luYear   = $luYear2;  $luMonth  = $luMonth2;   $luDay    = $luDay2;
				$luHour   = $luHour2;  $luMinute = $luMinute2;  $luSecond = $luSecond2;
				$luMicro  = $luMicro2;
			}
			push(@newId, $id) if ($timeStampStr2 gt $timeStampStr);
		} else {
			printf "*D* error parsing lastUpdated\n" if ($debug>0);
		}
	}
}
# update timestamp
printf "*D* maxObserved = %04d-%02d-%02dT%02d:%02d:%02d.%03d+%s\n",
	$luYear,$luMonth,$luDay, $luHour,$luMinute,$luSecond, $luMicro, $luTz if ($debug>0);
if ($update) {
	printf "*D* updating timestamp\n" if ($debug>0);
	open FD, ">$timeStampFile" or die "error opening timestamp file for update ($timeStampFile): $!";
	printf FD "%04d-%02d-%02dT%02d:%02d:%02d.%03d+%s",
		$luYear, $luMonth, $luDay, $luHour, $luMinute, $luSecond, $luMicro, $luTz;
	close(FD);
}

# list of IDs to be processed
printf "INFO: modified Patient IDs = %s\n", join(', ',@newId);

exit(0) if ($query);

# only end up here if actually requesting CRIDs
if (! defined($ccn)) {
	print "ERROR: must define CCN for requesting CRID\n";
	exit(-1);
}

# loop for each Patient ID
my ($patient, $crid);
push (@ptId, @newId);
foreach my $ptId (@ptId) {

# get Patient resource from FHIR server
print "*D* getting patient $ptId from server $fhirServer\n" if ($debug>0);
my $fhirClient = REST::Client->new();
$fhirPatientUrl = $fhirServer . '/Patient/' . $ptId;
$fhirClient->GET($fhirPatientUrl);
my $patientJson = $fhirClient->responseContent();
$patient = decode_json($patientJson);
print Dumper($patient) if ($debug>3);

# make sure we have a Patient resource
my $resourceType = $$patient{resourceType};
if (!defined($resourceType) || ($resourceType ne 'Patient')) {
	printf "ERROR: non-Patient resource for Patient ID %s\n", $ptId;
	next;
}

# check for already-existing CRID identifier
my $existingCrid;
my $identifier = $$patient{identifier};
if (defined($identifier)) {
 	foreach my $id (@$identifier) {
		$existingCrid = $$id{value} if ($$id{system} eq $CridUri);
	}
}

# map Patient resource to CRID service input
my $patientInfo = parsePatient($patient);
# normalize gender to birthSex, then gender
$$patientInfo{gender} = $$patientInfo{birthSex} if defined($$patientInfo{birthSex});
printf "INFO: given=%s, family=%s, gender=%s, dob=%s\n", 
	$$patientInfo{firstName}//'', $$patientInfo{lastName}//'', $$patientInfo{gender}//'', $$patientInfo{birthDate}//'';
# create body content
my $bodyContent = sprintf("{\n\t\"ccn\": %s,\n\t\"patient\": {\n", $ccn);
my @keys = keys(%$patientInfo);
for my $k ( 0 .. $#keys ) {
	my $key = $keys[$k];
	my $value = $$patientInfo{$key};
	$value = $debugGiven if (($key eq 'firstName') && (defined($debugGiven)));
	$value = $debugFamily if (($key eq 'lastName') && (defined($debugFamily)));
	$value = $debugDoB if (($key eq 'birthDate') && (defined($debugDoB)));
	$value = $debugGender if (($key eq 'gender') && (defined($debugGender)));
	$bodyContent .= sprintf("\t\t\"%s\": \"%s\"", $key, $value);
	$bodyContent .= ',' if ($k != $#keys);
	$bodyContent .= "\n";
}
$bodyContent .= "\t}\n}\n";
printf "*D* bodyContent=%s\n", $bodyContent if ($debug>0);
#exit;

# call CRID service
my $cridClient = REST::Client->new();
$patientJson = encode_json($patientInfo);
my $cridURL = $cridRestService;
printf "*D* cridURL = %s\n", $cridURL if ($debug>0);
my %header = ( "Content-Type","application/json" );
$cridClient->PUT($cridURL, $bodyContent, \%header);
my $cridJson = $cridClient->responseContent();
my $status = $cridClient->responseCode();
printf "*D* status=%s, cridJson=%s\n", $status, $cridJson if ($debug>0);
if ($status >= 300) {
	printf "ERR: status=%s from CRID REST service\n", $status;
	next;
}

# parse out response
my $response = decode_json($cridJson);
decodeRef($response) if ($debug>1);

# sort out match
my ($perfectCrid, @fuzzyCrid, @hiddenCrid);
my $nPerfect = 0;
my $nFuzzy = 0;
my $nHidden = 0;
my $generatedCrid = $$response{crid};
my $perfect = $$response{perfectMatch};
if (defined($perfect)) {
	my @perfect = @$perfect;
	$nPerfect = $#perfect + 1;
	my $hash = $perfect[0];
	my $matchType = $$hash{matchType};
	$perfectCrid = $$hash{crid};
}
my $fuzzy = $$response{fuzzyMatch};
if (defined($fuzzy)) {
	foreach my $f (@$fuzzy) {
		$nFuzzy++;
		push(@fuzzyCrid, $$f{crid});
	}
}
my $hidden = $$response{hiddenMatch};
if (defined($hidden)) {
	foreach my $f (@$hidden) {
		$nHidden++;
		push(@hiddenCrid, $$f{crid});
	}
}
printf "INFO: #perfect=%s, #fuzzy=%s, #hidden=%s\n", $nPerfect, $nFuzzy, $nHidden;
printf "INFO: crid=%s, existingCrid=%s, perfectCrid=%s\n", $crid//'', $existingCrid//'', $perfectCrid//'';
printf "INFO: fuzzyCrids = %s\n", join(', ',@fuzzyCrid) if ($nFuzzy > 0);
printf "INFO: hiddenCrids = %s\n", join(', ',@hiddenCrid) if ($nHidden > 0);

# detect unusual conditions
printf "ERROR: multiple (%s) perfect matches\n", $nPerfect 
	if ($nPerfect > 1);
printf "ERROR: no perfect match and no generated CRID\n" 
	if ($nPerfect==0 && !defined($generatedCrid));
printf "ERROR: perfect match (%s) and generated CRID (%s)\n", $perfectCrid,$generatedCrid 
	if ($nPerfect==1 && defined($generatedCrid));
printf "ERROR: differing existing (%s) and generated (%s) CRIDs\n", $existingCrid,$generatedCrid 
	if (defined($existingCrid) && defined($generatedCrid) && ($existingCrid != $generatedCrid));
if ( ($nPerfect > 1) 
  || ($nPerfect==0 && !defined($generatedCrid))
  || ($nPerfect==1 && !defined($perfectCrid))
  || ($nPerfect==1 && defined($generatedCrid))
  || (defined($perfectCrid)  && defined($generatedCrid))
# || (defined($existingCrid) && defined($generatedCrid))
  || (defined($existingCrid) && defined($perfectCrid)   && ($existingCrid != $perfectCrid))
  || (defined($existingCrid) && defined($generatedCrid) && ($existingCrid != $generatedCrid))
	) {
	next;
}
my $crid = $existingCrid // $perfectCrid // $generatedCrid;

# PUT identifier back on server
if (defined($existingCrid)) {
	printf "INFO: not updating existing CRID\n";
} else {
	# add new identifier
	my $identifier = $$patient{identifier};
	my @identifier = @$identifier;
	my %cridIdentifier = (
		"use" => "official",
		"type" => "REG",
		"system" => $CridUri,
		"value" => "$crid",
		);
	push(@identifier,\%cridIdentifier);
	$$patient{identifier} = \@identifier;
	print Dumper($patient) if ($debug>3);

	# encode to JSON && PUT resource on server
	my $json = encode_json($patient);
	my $urlFull = $fhirServer . '/Patient/' . $ptId;
	printf "*D* putting resource back to %s\n", $urlFull if ($debug>0);
	$fhirClient->PUT($urlFull, $json) or warn "error PUTting resource onto server\n";
}

} # end foreach @ptId


#######################################################################

sub tabs {
	my ($n) = @_;
	$n = 0 if (!defined($n));
	for (my $i = 0; $i <= $n; $i++) {print "\t";};
}

sub decodeRef {
	my ($ref, $level) = @_;
	$level = 0 if (!defined($level));

	if ((ref($ref) eq 'SCALAR') || (ref($ref) eq '')) {
		tabs($level);
		printf "%s\n", $ref;
	} elsif (ref($ref) eq 'ARRAY') {
		my @arr = @$ref;
		for (my $i = 0; $i <= $#arr; $i++) {
			tabs($level);
			printf "ArrayIndex=%s\n", $i;
			my $ref2 = $arr[$i];
			decodeRef($ref2, $level+1);
		}
	} elsif (ref($ref) eq 'HASH') {
		my %hash = %$ref;
		foreach my $k (keys(%hash)) {
			tabs($level);
			printf "HashKey=%s\n", $k;
			my $ref2 = $hash{$k};
			decodeRef($ref2, $level+1);
		}
	}
}

