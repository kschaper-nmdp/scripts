#!/usr/bin/perl
#
# package with various routines to interact with the CRID service
#
# usage:
#	use citCridService;
#
#	$matches = cridSearch($env, $stack, $patientInfo);
#		$env -- SQL environment (P|E|Q|D)
#		$stack -- SQL stack (0|1|2)
#		$patientInfo -- hash reference
#			given, family, birthDate, gender
#		$matches -- hash reference
#			$nPM, $nFM, @PM, @FM
#
# 2018-04-26 - ks - original
#

package citCridService;
use strict;
use warnings;
use Exporter;
use citSqlServer;

#use vars qw(@ISA @EXPORT);
use Exporter;
our @ISA = 'Exporter';
our @EXPORT = qw(cridSearch assignCrid $cridDebug);

our $cridDebug = 0;

#sub cridDebug {
#	my ($dbg) = $_;
#	$debug = $dbg;
#}

##################################################################################
# search for matching patient
sub cridSearch {
	my ($env, $stack, $ccn, $patientInfo) = @_;
	my %matches;

	printf "*D* cridSearch: env=%s, stack=%s\n", $env, $stack if ($cridDebug>0);
	my $dbh = citConnect($env,$stack);

	my ($PM1, $PM2, $PM3, $PM4, $PM5, $PM10);
	my ($FM1, $FM2, $FM3, $FM4, $FM5, $FM6);
	my $nPM = 0;
	my $nFM = 0;
	my ($sql, $qry, $crid);
	my @pMatchedCrids;
	my @fMatchedCrids;

	my $ccnDef = (defined($ccn)) ? 1 : 0;

	# different values used for matching
	my $ssnDef = (defined($$patientInfo{ssn})) ? 1 : 0;
	my $dobDef = (defined($$patientInfo{birthDate})) ? 1 : 0;
	my $sexDef = (defined($$patientInfo{gender})) ? 1 : 0;
	my $fnDef =  (defined($$patientInfo{given})) ? 1 : 0;
	my $lnDef =  (defined($$patientInfo{family})) ? 1 : 0;
	my $mmnDef = (defined($$patientInfo{mothersMaidenName})) ? 1 : 0;
	my $ridDef = (defined($$patientInfo{nmdpRid})) ? 1 : 0;
	my $iubmidDef =  (defined($$patientInfo{iubmid})) ? 1 : 0;
	my $teamIdDef =  (defined($$patientInfo{teamId})) ? 1 : 0;
	my $ebmtIdDef =  (defined($$patientInfo{ebmtId})) ? 1 : 0;
	my $ebmtCicDef = (defined($$patientInfo{ebmtCic})) ? 1 : 0;

	#--------------------------------------------------------------

	# PM1: SSN, DOB, Gender
	if ($ssnDef && $dobDef && $sexDef) {
		$sql = 'select unique_patient_id from t_unique_patient where ssn=? and birthday=? and gender=?';
		$qry = $dbh->prepare($sql);
		printf "*D* cridSearch: PM1: sql=%s [%s, %s, %s]\n", $sql, $$patientInfo{ssn}, $$patientInfo{birthDate}, $$patientInfo{gender} if ($cridDebug>2);
		$qry->execute($$patientInfo{ssn}, $$patientInfo{birthDate}, $$patientInfo{gender});
		$qry->bind_columns( undef, \$crid );
		my $nMatch = 0;
		while ($qry->fetch()) {
			push(@pMatchedCrids, ['PM1',$crid]);
			$nPM++;
			$nMatch++;
		}
		printf "*D* cridSearch: PM1: nMatch=%d\n",$nMatch if ($cridDebug>2);
	} else {
		print "*D* cridSearch: skipping PM1\n" if ($cridDebug>1);
	}

	# PM2: DOB, FirstName, LastName, Gender, MothersMaidenName
	if ($dobDef && $fnDef && $lnDef && $mmnDef) {
		$sql = 'select unique_patient_id from t_unique_patient where birthday=? and first_name=? and last_name=? and mothers_maiden_name==?';
		$qry = $dbh->prepare($sql);
		printf "*D* cridSearch: PM2: sql=%s [%s, %s, %s, %s]\n", $sql, $$patientInfo{birthDate}, $$patientInfo{given}, $$patientInfo{family}, $$patientInfo{mothersMaidenName} if ($cridDebug>2);
		$qry->execute($$patientInfo{birthDate}, $$patientInfo{given}, $$patientInfo{family}, $$patientInfo{mothersMaidenName});
		$qry->bind_columns( undef, \$crid );
		my $nMatch = 0;
		while ($qry->fetch()) {
			push(@pMatchedCrids, ['PM2',$crid]);
			$nPM++;
			$nMatch++;
		}
		printf "*D* cridSearch: PM2: nMatch=%d\n",$nMatch if ($cridDebug>2);
	} else {
		print "*D* cridSearch: skipping PM2\n" if ($cridDebug>1);
	}

	# PM3: DOB, Gender, RID
	if ($dobDef && $sexDef && $ridDef) {
		$sql = 'select unique_patient_id from t_unique_patient where birthday=? and gender=? and nmdp_rid=?';
		$qry = $dbh->prepare($sql);
		printf "*D* cridSearch: PM3: sql=%s [%s, %s, %s]\n", $sql, $$patientInfo{birthDate}, $$patientInfo{gender}, $$patientInfo{nmdpRid} if ($cridDebug>2);
		$qry->execute($$patientInfo{birthDate}, $$patientInfo{gender}, $$patientInfo{nmdpRid});
		$qry->bind_columns( undef, \$crid );
		my $nMatch = 0;
		while ($qry->fetch()) {
			push(@pMatchedCrids, ['PM3',$crid]);
			$nPM++;
			$nMatch++;
		}
		printf "*D* cridSearch: PM3: nMatch=%d\n",$nMatch if ($cridDebug>2);
	} else {
		print "*D* cridSearch: skipping PM3\n" if ($cridDebug>1);
	}

	# PM4: DOB, Gender, CIBMTR IUBMID + Team
	if ($dobDef && $sexDef && $iubmidDef && $teamIdDef) {
		$sql = 'select unique_patient_id from t_unique_patient where birthday=? and gender=? and cibmtr_iubmid=? and cibmtr_team=?';
		$qry = $dbh->prepare($sql);
		printf "*D* cridSearch: PM4: sql=%s [%s, %s, %s, %s]\n", $sql, $$patientInfo{birthDate}, $$patientInfo{gender}, $$patientInfo{iubmid}, $$patientInfo{teamId} if ($cridDebug>2);
		$qry->execute($$patientInfo{birthDate}, $$patientInfo{gender}, $$patientInfo{iubmid}, $$patientInfo{teamId});
		$qry->bind_columns( undef, \$crid );
		my $nMatch = 0;
		while ($qry->fetch()) {
			push(@pMatchedCrids, ['PM4',$crid]);
			$nPM++;
			$nMatch++;
		}
		printf "*D* cridSearch: PM4: nMatch=%d\n",$nMatch if ($cridDebug>2);
	} else {
		print "*D* cridSearch: skipping PM4\n" if ($cridDebug>1);
	}

	# PM5: DOB, Gender, EBMT ID + CIC
	if ($dobDef && $sexDef && $ebmtIdDef && $ebmtCicDef) {
		$sql = 'select unique_patient_id from t_unique_patient where birthday=? and gender=? and ebmt_id=? and ebmt_cic=?';
		$qry = $dbh->prepare($sql);
		printf "*D* cridSearch: PM5: sql=%s [%s, %s, %s, %s]\n", $sql, $$patientInfo{birthDate}, $$patientInfo{gender}, $$patientInfo{ebmtId}, $$patientInfo{ebmtCic} if ($cridDebug>2);
		$qry->execute($$patientInfo{birthDate}, $$patientInfo{gender}, $$patientInfo{ebmtId}, $$patientInfo{ebmtCic});
		$qry->bind_columns( undef, \$crid );
		my $nMatch = 0;
		while ($qry->fetch()) {
			push(@pMatchedCrids, ['PM5',$crid]);
			$nPM++;
			$nMatch++;
		}
		printf "*D* cridSearch: PM5: nMatch=%d\n",$nMatch if ($cridDebug>2);
	} else {
		print "*D* cridSearch: skipping PM5\n" if ($cridDebug>1);
	}

	# PM10: DOB, Gender, FirstName, LastName (Epic FHIR match)
	if ($dobDef && $sexDef && $fnDef && $lnDef) {
		$sql = 'select unique_patient_id from t_unique_patient where birthday=? and gender=? and first_name=? and last_name=?';
		$qry = $dbh->prepare($sql);
		printf "*D* cridSearch: PM10: sql=%s [%s, %s, %s, %s]\n", $sql, $$patientInfo{birthDate}, $$patientInfo{gender}, $$patientInfo{given}, $$patientInfo{family} if ($cridDebug>2);
		$qry->execute($$patientInfo{birthDate}, $$patientInfo{gender}, $$patientInfo{given}, $$patientInfo{family});
		$qry->bind_columns( undef, \$crid );
		my $nMatch = 0;
		while ($qry->fetch()) {
			push(@pMatchedCrids, ['PM10',$crid]);
			$nPM++;
			$nMatch++;
		}
		printf "*D* cridSearch: PM10: nMatch=%d\n",$nMatch if ($cridDebug>2);
	} else {
		print "*D* cridSearch: skipping PM10 \n" if ($cridDebug>1);
	}

	#--------------------------------------------------------------

	# FM1: SSN
	if ($ssnDef) {
		$sql = 'select unique_patient_id from t_unique_patient where ssn=?';
		$qry = $dbh->prepare($sql);
		printf "*D* cridSearch: FM1: sql=%s [%s]\n", $sql, $$patientInfo{ssn} if ($cridDebug>2);
		$qry->execute($$patientInfo{ssn});
		$qry->bind_columns( undef, \$crid );
		my $nMatch = 0;
		while ($qry->fetch()) {
			push(@fMatchedCrids, ['FM1',$crid]);
			$nFM++;
			$nMatch++;
		}
		printf "*D* cridSearch: FM1: nMatch=%d\n",$nMatch if ($cridDebug>2);
	} else {
		print "*D* cridSearch: skipping FM1\n" if ($cridDebug>1);
	}

	# FM2: 4 of (FirstName, LastName, DOB, Gender, MothersMaidenName)
	if (($fnDef + $lnDef + $dobDef + $sexDef + $mmnDef)>=4) {
		$sql = 'select unique_patient_id from t_unique_patient where (select
		case when first_name=? then 1 else 0 end 	+
		case when last_name=? then 1 else 0 end 	+
		case when birthday=? then 1 else 0 end 	+
		case when gender=? then 1 else 0 end 	+
		case when mothers_maiden_name=? then 1 else 0 end
		) >= 4';
		$qry = $dbh->prepare($sql);
		printf "*D* cridSearch: FM2: sql=%s [%s, %s, %s, %s, %s]\n", $sql, 
			$$patientInfo{given}//'undef', $$patientInfo{family}//'undef', $$patientInfo{birthDate}//'undef', $$patientInfo{gender}//'undef', 
			$$patientInfo{mothersMaidenName}//'undef' if ($cridDebug>2);
		$qry->execute($$patientInfo{given}, $$patientInfo{family}, $$patientInfo{birthDate}, $$patientInfo{gender}, 
			$$patientInfo{mothersMaidenName});
		$qry->bind_columns( undef, \$crid );
		my $nMatch = 0;
		while ($qry->fetch()) {
			push(@fMatchedCrids, ['FM2',$crid]);
			$nFM++;
			$nMatch++;
		}
		printf "*D* cridSearch: FM2: nMatch=%d\n",$nMatch if ($cridDebug>2);
	} else {
		print "*D* cridSearch: skipping FM2\n" if ($cridDebug>1);
	}

	# FM3: DOB, Gender, and 1 of (FirstName, LastName, MothersMaidenName)
	if ($dobDef && $sexDef && ($fnDef + $lnDef + $mmnDef)>=1) {
		$sql = 'select unique_patient_id from t_unique_patient
		where birthday = ? and gender = ?
		and (select
		case when first_name=? then 1 else 0 end 	+
		case when last_name=? then 1 else 0 end 	+
		case when mothers_maiden_name=? then 1 else 0 end
		) >= 1';
		$qry = $dbh->prepare($sql);
		printf "*D* cridSearch: FM3: sql=%s [%s, %s, %s, %s, %s]\n", $sql, 
			$$patientInfo{birthDate}, $$patientInfo{gender}, $$patientInfo{given}//'undef', $$patientInfo{family}//'undef', 
			$$patientInfo{mothersMaidenName}//'undef' if ($cridDebug>2);
		$qry->execute($$patientInfo{birthDate}, $$patientInfo{gender}, $$patientInfo{given}, $$patientInfo{family}, 
			$$patientInfo{mothersMaidenName});
		$qry->bind_columns( undef, \$crid );
		my $nMatch = 0;
		while ($qry->fetch()) {
			push(@fMatchedCrids, ['FM3',$crid]);
			$nFM++;
			$nMatch++;
		}
		printf "*D* cridSearch: FM3: nMatch=%d\n",$nMatch if ($cridDebug>2);
	} else {
		print "*D* cridSearch: skipping FM3\n" if ($cridDebug>1);
	}

	# FM4: RID and 1 of (DOB, Gender)
	if ($ridDef && ($dobDef + $sexDef)>=1) {
		$sql = 'select unique_patient_id from t_unique_patient
		where nmdp_rid = ?
		and (select
		case when birthday=? then 1 else 0 end 	+
		case when gender=? then 1 else 0 end
		) >= 1';
		$qry = $dbh->prepare($sql);
		printf "*D* cridSearch: FM4: sql=%s [%s, %s, %s]\n", $sql, 
			$$patientInfo{nmdpRid}, $$patientInfo{birthDate}//'undef', $$patientInfo{gender}//'undef' if ($cridDebug>2);
		$qry->execute($$patientInfo{nmdpRid}, $$patientInfo{birthDate}, $$patientInfo{gender});
		$qry->bind_columns( undef, \$crid );
		my $nMatch = 0;
		while ($qry->fetch()) {
			push(@fMatchedCrids, ['FM4',$crid]);
			$nFM++;
			$nMatch++;
		}
		printf "*D* cridSearch: FM4: nMatch=%d\n",$nMatch if ($cridDebug>2);
	} else {
		print "*D* cridSearch: skipping FM4\n" if ($cridDebug>1);
	}

	# FM5: CIBMTR IUBMID + Team, and 1 of (DOB, Gender)
	if ($iubmidDef && $teamIdDef && ($dobDef + $sexDef)>=1) {
		$sql = 'select unique_patient_id from t_unique_patient
		where cibmtr_iubmid = ? and cibmtr_team = ?
		and (select
		case when birthday=? then 1 else 0 end 	+
		case when gender=? then 1 else 0 end
		) >= 1';
		$qry = $dbh->prepare($sql);
		printf "*D* cridSearch: FM5: sql=%s [%s, %s, %s, %s]\n", $sql, $$patientInfo{iubmid}, $$patientInfo{teamId}, 
			$$patientInfo{birthDate}//'undef', $$patientInfo{gender}//'undef' if ($cridDebug>2);
		$qry->execute($$patientInfo{iubmid}, $$patientInfo{teamId}, $$patientInfo{birthDate}, $$patientInfo{gender});
		$qry->bind_columns( undef, \$crid );
		my $nMatch = 0;
		while ($qry->fetch()) {
			push(@fMatchedCrids, ['FM5',$crid]);
			$nFM++;
			$nMatch++;
		}
		printf "*D* cridSearch: FM5: nMatch=%d\n",$nMatch if ($cridDebug>2);
	} else {
		print "*D* cridSearch: skipping FM5\n" if ($cridDebug>1);
	}

	# FM6: EBMT ID + CIC, and 1 of (DOB, Gender)
	if ($iubmidDef && $teamIdDef && ($dobDef + $sexDef)>=1) {
		$sql = 'select unique_patient_id from t_unique_patient
		where cibmtr_iubmid = ? and cibmtr_team = ?
		and (select
		case when birthday=? then 1 else 0 end 	+
		case when gender=? then 1 else 0 end
		) >= 1';
		$qry = $dbh->prepare($sql);
		printf "*D* cridSearch: FM6: sql=%s [%s, %s, %s, %s]\n", $sql, $$patientInfo{ebmtId}, $$patientInfo{ebmtCic}, 
			$$patientInfo{birthDate}//'undef', $$patientInfo{gender}//'undef' if ($cridDebug>2);
		$qry->execute($$patientInfo{ebmtId}, $$patientInfo{ebmtCic}, $$patientInfo{birthDate}, $$patientInfo{gender});
		$qry->bind_columns( undef, \$crid );
		my $nMatch = 0;
		while ($qry->fetch()) {
			push(@fMatchedCrids, ['FM6',$crid]);
			$nFM++;
			$nMatch++;
		}
		printf "*D* cridSearch: FM6: nMatch=%d\n",$nMatch if ($cridDebug>2);
	} else {
		print "*D* cridSearch: skipping FM6\n" if ($cridDebug>1);
	}

	$matches{nPM} = $nPM;
	$matches{nFM} = $nFM;
	$matches{PM} = \@pMatchedCrids;
	$matches{FM} = \@fMatchedCrids;
	return(\%matches);
}


##################################################################################
# assign CRID to patient
#
# TBD - what id to use for lst_updt_id -- something from Patient resource?

sub assignCrid {
	my ($env, $stack, $ccn, $patientInfo) = @_;

	printf "*D* assignCrid: env=%s, stack=%s\n", $env, $stack if ($cridDebug>0);
	my $dbh = citConnect($env,$stack);

	my ($year, $month, $day, $hour, $minute, $second) = split /\s+/, `date '+%Y %m %d %H %M %S'`;
	my $now = "$year-$month-$day $hour:$minute:$second";

	my ($sql, $qry, $crid);
	my $updateId = 'FHIR';

	# get next CRID value
	$sql = 'select max(unique_patient_id) from t_unique_patient';
	$qry = $dbh->prepare($sql)
		or die "Couldn't prepare statement to get next CRID: " . DBI->errstr;
	$qry->execute()
		or die "Couldn't execute statement to get next CRID: " . DBI->errstr;
	$qry->bind_columns( undef, \$crid );
	$qry->fetch();
	$crid++;
	printf "*D* assignCrid: next CRID value is %s\n", $crid if ($cridDebug>0);

	# sort out gender
	my $finalGender = $$patientInfo{birthSex};
	$finalGender = $$patientInfo{gender} if (!defined($finalGender));

	# insert row into t_unique_patient, trigger gets it to t_patient
	# (insufficient information for t_patient_event, t_infus, or t_infus_dnr)
	$sql ='insert into t_unique_patient (unique_patient_id, cibmtr_center_number, active
	, birthday, gender, first_name, last_name,   lst_updt_dte, lst_updt_id)
	values (?,?,?, ?,?,?,?, ?,?)';

	printf "*D* assignCrid: sql=%s [%s,%s,%s, %s,%s,%s,%s, %s,%s]\n",
		$sql, $crid,$ccn,1, $$patientInfo{birthDate},$finalGender,$$patientInfo{given},$$patientInfo{family}, $now,$updateId
		if ($cridDebug>0);

	$dbh->do($sql, undef, $crid,$ccn,1,
		$$patientInfo{birthDate}, $finalGender, $$patientInfo{given}, $$patientInfo{family},
		$now, $updateId);

	return($crid);
}

1
