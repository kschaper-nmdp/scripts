#!/usr/bin/perl

use Dancer;
use JSON::Parse ':all';
use citSqlServer;
use Data::Dumper;
my $env = 'D';
my $debug = 9;

# change default settings
set logger => 'file';
set log_file => 'service.log';
set log => 'debug';

hook 'before' => sub {
	debug "before: path=".request->path_info if ($debug>2);
};

# helpers
sub getEventId {
	my ($ccn, $crid, $eventDate) = @_;
	debug "getEventId: ccn=$ccn, crid=$crid, eventDate=$eventDate" if ($debug>0);
	my $dbh = citConnect($env);
	my $sql = 'select patient_event_id from t_patient_event where cibmtr_center_number=? and unique_patient_id=? and event_dte=?';
	my $qry = $dbh->prepare($sql);
	$qry->execute($ccn, $crid, $eventDate);
	my $eventId;
	$qry->bind_columns(undef, \$eventId);
	$qry->fetch();
	return $eventId // 0;
}

############################################################################
# routes
############################################################################

my ($dbh, $sql, $qry);

# debug
# curl http://localhost:3000/debug/1
any '/debug/:dbg' => sub {
	$debug = param('dbg');
};

# env
# curl http://localhost3000/env/D
any '/env/:env' => sub {
	$env = param('env');
	if ($env ne 'P' && $env ne 'I' && $env ne 'Q' && $env ne 'V' && $env ne 'D') {
		return 'unknown env='.$env;
	} else {
		return "using ".$env." environment";
	}
};

# hello
any '/hello' => sub {
	return "hello";
};
any '/hello/:name' => sub {
	my $name = param('name');
	return "hello ".$name;
};

# get eventId
# curl 'http://localhost:3000/eventId/\{"ccn":"10151","crid":"3867951","eventDate":"2017-12-01"\}'
get '/eventId/:info' => sub {
	my $info = param('info');
	debug "info=".$info if ($debug>2);
	return "info is not valid JSON" if (! valid_json($info));
	my $json = parse_json($info);
	my ($ccn, $crid, $eventDate);
	foreach my $key (keys %$json) {
		debug "eventId.info " . $key . ' = ' . $$json{$key} if ($debug>2);
		$ccn = $$json{$key} if ($key eq 'ccn');
		$crid = $$json{$key} if ($key eq 'crid');
		$eventDate = $$json{$key} if ($key eq 'eventDate');
	}
	if (!defined($ccn) || !defined($crid) || !defined($eventDate)) {
		return "missing key info" . (defined($ccn))?'':':ccn' . (defined($crid))?'':':crid' . (defined($eventDate))?'':':eventDate' ;
	}
	my $eventId = getEventId($ccn, $crid, $eventDate);
	return $eventId;
};
# put datapoint
# curl 'http://localhost:3000/dataPoint/\{"eventId":410335,"object":"HCT","property":"\[type\]","value":1\}'

any '/dataPoint/:info' => sub {
	my $info = param('info');	# {eventId:xxx, object:xxx, property:[xxx], timepoint:xxx, instance:xxx, value:xxx}

	# extract identifiers
	return "info is not valid JSON" if (! valid_json($info));
	my $json = parse_json($info);
	my ($eventId, $object, @property, $timepoint, $instance, $value, $property);
	foreach my $key (keys %$json) {
		debug "info " . $key . ' = ' . $$json{$key} if ($debug>2);
		$eventId = $$json{$key} if ($key eq 'eventId');
		$object = $$json{$key} if ($key eq 'object');
		@property = @$json{$key} if ($key eq 'property');
		$timepoint = $$json{$key} if ($key eq 'timepoint');
		$value = $$json{$key} if ($key eq 'value');
		$instance = $$json{$key} if ($key eq 'instance');
	}
	if (!defined($eventId) || !defined($object) || !defined($value)) {
		return "must specify eventId, object, and value";
	}
	$instance = 1 if (!defined($instance));
	@property = sort @property if ($#property>=0);
	$property = join (':', @property);
	$property =~ s/^.*\[//;
	$property =~ s/\].*$//;
	debug "object=".$object.", property=".$property if ($debug>0);
					  
	# get datadicts
	$dbh = citConnect($env);
	$sql = 'select dd.data_dict_id from kirt.t_datapoint dp
		join kirt.t_datapoint_datadict dpd on dp.id = dpd.datapoint_id
		join dbo.t_data_dictionary dd on dpd.data_dict_id = dd.data_dict_id
		where dp.object=? and dp.property=?';
	$qry = $dbh->prepare($sql);
	print "*D* sql=$sql [$object, $property]\n" if ($debug>8);
	$qry->execute($object, $property);
	my ($dataDictId, @dataDictId);
	$qry->bind_columns(undef, \$dataDictId);
	while ($qry->fetch()) {
		print "*D* dataDictId=$dataDictId\n" if ($debug>8);
		debug "dataDictId=".$dataDictId if ($debug>5);
		push(@dataDictId, $dataDictId);
	}
	debug "dataDictIds=".join('; ',@dataDictId) if ($debug>0);

	# get datapoints on all event-related forms
	my $dataDictList = join(',',@dataDictId);
	$sql = "declare \@PE int = ?;
		select fmt.frm_id, fd.frm_maj_version, fmt.fuf_seq_num, fmt.sts_cde, fld.field_name, dd.data_type, d.answer
		from t_patient_event pe
		join t_frm_track fmt on pe.cibmtr_center_number=fmt.ctr_cde and pe.unique_patient_id=fmt.nmdp_id and pe.event_dte=fmt.event_dte
		join t_frm_def fd on fmt.frm_def_id=fd.frm_def_id
		join t_field_def fld on fmt.frm_def_id = fld.frm_def_id
		join t_data_dictionary dd on fld.data_dict_id = dd.data_dict_id
		left join t_frm_data_str d on fmt.frm_track_id=d.context_id and fld.field_def_id=d.field_def_id and d.inst_num=1
		where pe.patient_event_id=\@PE and fld.data_dict_id in ($dataDictList)
		union
		select fmt.frm_id, fd.frm_maj_version, fmt.fuf_seq_num, fmt.sts_cde, fld.field_name, dd.data_type
		, ltrim(str(d.answer,50,isnull(dd.sig_digits,0)))
		from t_patient_event pe
		join t_frm_track fmt on pe.cibmtr_center_number=fmt.ctr_cde and pe.unique_patient_id=fmt.nmdp_id and pe.event_dte=fmt.event_dte
		join t_frm_def fd on fmt.frm_def_id=fd.frm_def_id
		join t_field_def fld on fmt.frm_def_id = fld.frm_def_id
		join t_data_dictionary dd on fld.data_dict_id = dd.data_dict_id
		left join t_frm_data_num d on fmt.frm_track_id=d.context_id and fld.field_def_id=d.field_def_id and d.inst_num=1
		where pe.patient_event_id=\@PE and fld.data_dict_id in ($dataDictList)
		union
		select fmt.frm_id, fd.frm_maj_version, fmt.fuf_seq_num, fmt.sts_cde, fld.field_name, dd.data_type
		, case when fd.fn2_era = 1 then convert(varchar(25),d.answer,110)
		  when fld.control_type='Time' then replace(convert(varchar(5),d.answer,108),':','') -- HHMM
		  when fld.control_type='TextBox' then convert(varchar(4),d.answer,126) -- YYYY
		  else convert(varchar(25), d.answer, 110)  end
		from t_patient_event pe
		join t_frm_track fmt on pe.cibmtr_center_number=fmt.ctr_cde and pe.unique_patient_id=fmt.nmdp_id and pe.event_dte=fmt.event_dte
		join t_frm_def fd on fmt.frm_def_id=fd.frm_def_id
		join t_field_def fld on fmt.frm_def_id = fld.frm_def_id
		join t_data_dictionary dd on fld.data_dict_id = dd.data_dict_id
		left join t_frm_data_dte d on fmt.frm_track_id=d.context_id and fld.field_def_id=d.field_def_id and d.inst_num=1
		where pe.patient_event_id=\@PE and fld.data_dict_id in ($dataDictList)
		order by frm_id, frm_maj_version, fuf_seq_num";
	$qry = $dbh->prepare($sql);
	print "*D* sql=$sql [$eventId]\n" if ($debug>8);
	$qry->execute($eventId);
	my ($frmNum, $frmRev, $fuf, $stsCde, $fieldName, $dType, $answer);
	$qry->bind_columns(undef, \$frmNum, \$frmRev, \$fuf, \$stsCde, \$fieldName, \$dType, \$answer);
	printf "%4s %1s %3s %3s %1s %-50s %s\n", 'form','r','fuf','sts','t','fieldName','answer' if ($debug>0);
	while ($qry->fetch()) {
		printf "%4d %1d %3d %3s %1s %-50s %s\n", $frmNum, $frmRev, $fuf, $stsCde, $dType, $fieldName, $answer // 'NULL' if ($debug>0);
	}
	return
};
	 
any '/data/:info' => sub {
	my $info = param('info');
	debug "/data/info=".$info if ($debug>0);
};	

any '/data' => sub {
	debug "bare /data" if ($debug>0);
};	

# default route handler
any qr{.*} => sub {
	return "unknown route: ".request->path_info;
};

dance;
