with src as (
    select 
        *   
         ,'{{source('cmlsyspro','invmaster')}}' as etl_src_table_name
         ,'CMLSYSPRO'                                as source_system
    from {{source('cmlsyspro','invmaster')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by stockcode order by _rivery_last_update desc) row_nbr
    from src
)
select
	stockcode,
	description,
	longdesc,
	alternatekey1,
	alternatekey2,
	eccuser,
	stockuom,
	alternateuom,
	otheruom,
	convfactaltuom,
	convmuldiv,
	convfactothuom,
	muldiv,
	mass,
	volume,
	decimals,
	pricecategory,
	pricemethod,
	supplier,
	cyclecount,
	productclass,
	taxcode,
	othertaxcode,
	listpricecode,
	serialmethod,
	interfaceflag,
	kittype,
	lowlevelcode,
	buyer,
	planner,
	traceabletype,
	mpsflag,
	bulkissueflag,
	abcclass,
	leadtime,
	stockmovementreq,
	clearingflag,
	supercessiondate,
	abcanalysisreq,
	abccostingreq,
	costuom,
	minpricepct,
	labourcost,
	materialcost,
	fixoverhead,
	variableoverhead,
	partcategory,
	drawofficenum,
	warehousetouse,
	buyingrule,
	specificgravity,
	implosionnum,
	ebq,
	componentcount,
	fixtimeperiod,
	pansize,
	docktostock,
	outputmassflag,
	shelflife,
	version,
	release,
	demandtimefence,
	maketoorderflag,
	manufleadtime,
	grossreqrule,
	percentageyield,
	abcpreprod,
	abcmanufacturing,
	abcsales,
	abccumpreprod,
	abccummanuf,
	wipctlglcode,
	resourcecode,
	gsttaxcode,
	prcinclgst,
	serentryatsale,
	stpselection,
	userfield1,
	userfield2,
	userfield3,
	userfield4,
	userfield5,
	tariffcode,
	supplementaryunit,
	ebqpan,
	stdlandedcost,
	lctrequired,
	stdlctroute,
	issmultlotsflag,
	inclinstrvalid,
	stdlabcostsbill,
	phantomifcomp,
	countryoforigin,
	stockonhold,
	stockonholdreason,
	eccflag,
	stockandaltum,
	altunitchar,
	jobsonhold,
	jobholdallocs,
	purchonhold,
	salesonhold,
	maintonhold,
	batchbill,
	blanketpoexists,
	calloffbpoexists,
	distwarehousetouse,
	jobclassification,
	subcontractcost,
	datestkadded,
	inspectionflag,
	serialprefix,
	serialsuffix,
	returnableitem,
	productgroup,
	pricetype,
	basis,
	manualcostflag,
	manufactureuom,
	convfactmum,
	manmuldiv,
	lookaheadwin,
	loadingfactor,
	supplunitcode,
	storagesecurity,
	storagehazard,
	storagecondition,
	productshelflife,
	internalshelflife,
	altmethodflag,
	altsisoflag,
	altreductionflag,
	withtaxexpensetype,
	timestamp,
	usesprefsupplier,
	prdrecallflag,
	onholdreason,
	specificgravity6,
	suppunitfactor,
	suppunitsmuldiv,
	qminspectionreq,
	_rivery_river_id,
	_rivery_run_id,
	_rivery_last_update,
    etl_src_table_name,
 	source_system
from deduped
where row_nbr = 1