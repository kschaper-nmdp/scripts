#!/usr/bin/perl

# CRID REST service
#
# .../env/ENV
#		ENV -- environment, P|I|Q|V|D
#			P = production
#			I = integration
#			Q = Q/A
#			V|D = development (default)
# .../stack/STACK
#		STACK -- which stack, 0-2 (default is environment specific)
# .../debug/DEBUG
#		DEBUG -- debug level, 0-99 (default is 0)
# .../CRID/ccn/:ccn/:patient
#		GET - search
#			return: list of matches
#		POST - create
#			return: CRID + any FM matches
# .../CRID/123/:info
#		return CRID

use Dancer2;
use JSON::Parse ':all';
use citSqlServer;
use citCridService;
use Data::Dumper;

my $env = 'D';
my $stack = 2;
my $debug = 0;

# change default settings
#set logger => 'console';
set logger => 'file';
set log => 'core';
set log_file => 'service.log';

hook 'before' => sub {
	printf "*D* before: path=%s\n", request->path_info if ($debug>4);
#	debug "before: path=".request->path_info if ($debug>4);
};

# helpers
sub unpackMatch {
	# extract and return CRIDs from match results
	# match results is array of arrays of matchType & crid
	my $arrRef = (@_);
	printf "*D* unpackMatch: entry\n" if ($debug>4);
	print Dumper($arrRef) if ($debug>4);
	my %crid;
	foreach my $ar (@$arrRef) {
		print Dumper($ar) if ($debug>4);
		my $c = $$ar[1];
		printf "*D* unpackMatch: c=(%s,%s)\n", $$ar[0], $c if ($debug>4);
		$crid{$c} = 1;
	}
	printf "*D* unpackMatch: done foreach\n" if ($debug>4);
	my @crid = keys(%crid);
	printf "*D* unpackMatch: #crid=%d\n", $#crid if ($debug>4);
	return join(',', @crid);
}

############################################################################
# routes
############################################################################

# status
any '/status' => sub { return "env: $env; stack: $stack"; };

# debug
any '/debug' => sub { return $debug; };
any '/debug/:dbg' => sub {
	$debug = param('dbg');
	$cridDebug = $debug;
	return $debug;
};

# env
any '/env' => sub { return "using ".$env." environment"; };
any '/env/:env' => sub {
	$env = param('env');
	if ($env ne 'P' && $env ne 'I' && $env ne 'Q' && $env ne 'V' && $env ne 'D') {
		return 'unknown env='.$env;
	} else {
		return "using ".$env." environment";
	}
};

# stack
any '/stack' => sub { return "using stack ".$stack; };
any '/stack/:stack' => sub {
	$env = param('stack');
	if ($stack < 0 && $stack > 2) {
		return 'unknown stack='.$stack;
	} else {
		return "using stack ".$stack;
	}
};


###############
### CRID ###

# get: /CRID/ccn/:ccn/:patient
# put: /CRID/ccn/:ccn/:patient

# GET /CRID -- search
# curl 'http://localhost:3000/CRID/ccn/12345/\{"firstName":"Adam","lastName":"Smith","birthDate":"1980-01-01","gender":"m"\}'
get '/CRID/ccn/:ccn/:patient' => sub {
	printf "*D* GET: entry\n" if ($debug>0);
	# get input information
	my $ccn = param('ccn');
	my $patient = param('patient');
	return "GET: patient is not valid JSON" if (! valid_json($patient));
	my $json = parse_json($patient);
	my ($firstName, $lastName, $birthDate, $gender);
	foreach my $key (keys %$json) {
		$firstName = $$json{$key} if ($key eq 'firstName');
		$lastName = $$json{$key} if ($key eq 'lastName');
		$birthDate = $$json{$key} if ($key eq 'birthDate');
		$gender = $$json{$key} if ($key eq 'gender');
	}
	# validate
	my @error = ();
	push(@error, "missing ccn") if (!defined($ccn));
	push(@error, "missing firstName") if (!defined($firstName));
	push(@error, "missing lastName") if (!defined($lastName));
	push(@error, "missing birthDate") if (!defined($birthDate));
	push(@error, "missing gender") if (!defined($gender));
	push(@error, "ccn ($ccn) is not valid") if (defined($ccn) && ($ccn !~ /^\d\d\d\d\d$/));
	push(@error, "firstName ($firstName) is not valid") if (defined($firstName) && ($firstName !~ /^[A-Z][a-z]*$/));
	push(@error, "lastName ($lastName) is not valid") if (defined($lastName) && ($lastName !~ /^[A-Z][a-z]*$/));
	push(@error, "birthDate ($birthDate) is not valid") if (defined($birthDate) && ($birthDate !~ /^\d\d\d\d\-\d\d\-\d\d$/));
	push(@error, "gender ($gender) is not valid") if (defined($gender) && ($gender !~ /^[mMfF]$/));
	my $nError = $#error + 1;
	return sprintf("%d validation errors: %s", $nError, join('; ',@error)) if ($nError > 0);
	printf "*D* GET: ccn:%s firstName:%s lastName:%s birthDate:%s gender:%s",
		$ccn//'', $firstName//'', $lastName//'', $birthDate//'', $gender//'' if ($debug>0);
	# cridSearch
	my %patientInfo = ( given => $firstName, family => $lastName, birthDate => $birthDate, gender => $gender, );
	my $searchResults = cridSearch($env, $stack, $ccn, \%patientInfo);
	my $nPM = $$searchResults{nPM};		my $PM = $$searchResults{PM};
	my $nFM = $$searchResults{nFM};		my $FM = $$searchResults{FM};
	printf "*D* GET: nPM=%s, nFM=%s\n", $nPM, $nFM if ($debug>1);
	# ensure unique CRIDs
	my %unique = ();
	foreach my $ar (@$PM) { $unique{$$ar[1]} = 1; }
	my $PMstr = join(',', keys(%unique));
	%unique = ();
	foreach my $ar (@$FM) { $unique{$$ar[1]} = 1; }
	my $FMstr = join(',', keys(%unique));
	printf "*D* GET: PMstr=%s FMstr=%s\n", $PMstr,$FMstr if ($debug>0);
	# return
	if ($nPM+$nFM==0) {	return 'noMatch' } 
	elsif ($nPM>0) { return sprintf("(%s)PM=%s", $nPM,$PMstr) }
	elsif ($nFM>0) { return sprintf("(%s)FM=%s", $nFM,$FMstr) }
	else { return "mystery"; }
};

# PUT /CRID -- create
# curl 'http://localhost:3000/CRID/ccn/12345/\{"firstName":"Adam","lastName":"Smith","birthDate":"1980-01-01","gender":"m"\}'
put '/CRID/ccn/:ccn/:patient' => sub {
	printf "*D* PUT: entry\n" if ($debug>0);
	# get input information
	my $ccn = param('ccn');
	my $patient = param('patient');
	return "PUT: patient is not valid JSON" if (! valid_json($patient));
	my $json = parse_json($patient);
	my ($firstName, $lastName, $birthDate, $gender);
	foreach my $key (keys %$json) {
		$firstName = $$json{$key} if ($key eq 'firstName');
		$lastName = $$json{$key} if ($key eq 'lastName');
		$birthDate = $$json{$key} if ($key eq 'birthDate');
		$gender = $$json{$key} if ($key eq 'gender');
	}
	# validate
	my @error = ();
	push(@error, "missing ccn") if (!defined($ccn));
	push(@error, "missing firstName") if (!defined($firstName));
	push(@error, "missing lastName") if (!defined($lastName));
	push(@error, "missing birthDate") if (!defined($birthDate));
	push(@error, "missing gender") if (!defined($gender));
	push(@error, "ccn ($ccn) is not valid") if (defined($ccn) && ($ccn !~ /^\d\d\d\d\d$/));
	push(@error, "firstName ($firstName) is not valid") if (defined($firstName) && ($firstName !~ /^[A-Z][a-z]*$/));
	push(@error, "lastName ($lastName) is not valid") if (defined($lastName) && ($lastName !~ /^[A-Z][a-z]*$/));
	push(@error, "birthDate ($birthDate) is not valid") if (defined($birthDate) && ($birthDate !~ /^\d\d\d\d\-\d\d\-\d\d$/));
	push(@error, "gender ($gender) is not valid") if (defined($gender) && ($gender !~ /^[mMfF]$/));
	my $nError = $#error + 1;
	return sprintf("%d validation errors: %s", $nError, join('; ',@error)) if ($nError > 0);
	printf "*D* PUT: ccn:%s firstName:%s lastName:%s birthDate:%s gender:%s",
		$ccn//'', $firstName//'', $lastName//'', $birthDate//'', $gender//'' if ($debug>0);
	# cridSearch
	my %patientInfo = ( given => $firstName, family => $lastName, birthDate => $birthDate, gender => $gender, );
	my $searchResults = cridSearch($env, $stack, $ccn, \%patientInfo);
	my $nPM = $$searchResults{nPM};		my $PM = $$searchResults{PM};
	my $nFM = $$searchResults{nFM};		my $FM = $$searchResults{FM};
	# ensure unique CRIDs
	my %unique = ();
	foreach my $ar (@$PM) { $unique{$$ar[1]} = 1; }
	my $PMstr = join(',', keys(%unique));
	%unique = ();
	foreach my $ar (@$FM) { $unique{$$ar[1]} = 1; }
	my $FMstr = join(',', keys(%unique));
	printf "*D* PUT: PMstr=%s FMstr=%s\n", $PMstr,$FMstr if ($debug>0);
	# create CRID if no PM
	my $newCrid;
	if ($nPM == 0) {
		$newCrid = assignCrid($env, $stack, $ccn, \%patientInfo);
	}
	# return
	if ($nPM == 0) {
		my $str = "$newCrid (new";
		$str .= " FM:$FMstr" if ($nFM>0);
		$str .= ")";
		return $str; }
	elsif ($nPM == 1 ) { return "$PMstr (PM)"; }
	elsif ($nPM > 1 ) { return "ERROR: multiple perfect matches"; }
	else { return "mystery"; }
};

#############
### other ###

any '/data/:info' => sub {
	my $info = param('info');
	return "data: info=".$info;
};	

any '/data' => sub {
	return "data";
};	

# default route handler
any qr{.*} => sub {
	return "unknown route: ".request->path_info;
};

dance;
