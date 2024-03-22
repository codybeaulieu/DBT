with src as (
    select 
        *   
         ,'{{source('cmlsyspro','sormaster')}}' as etl_src_table_name
         ,'CMLSYSPRO'                                as source_system
    from {{source('cmlsyspro','sormaster')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by salesorder order by _rivery_last_update desc) row_nbr
    from src
)
select
	salesorder,
	nextdetailline,
	orderstatus,
	activeflag,
	cancelledflag,
	documenttype,
	customer,
	salesperson,
	customerponumber,
	orderdate,
	entrysystemdate,
	reqshipdate,
	datelastdocprt,
	shippinginstrs,
	shippinginstrscod,
	altshipaddrflag,
	invoicecount,
	invtermsoverride,
	creditauthority,
	branch,
	specialinstrs,
	entinvoice,
	entinvoicedate,
	discpct1,
	discpct2,
	discpct3,
	ordertype,
	taxexemptflag,
	area,
	taxexemptnumber,
	taxexemptoverride,
	cashcredit,
	warehouse,
	lastinvoice,
	scheduledordflag,
	gstexemptflag,
	gstexemptnum,
	gstexemptoride,
	ibtflag,
	ordacknwprinted,
	detcustmvmtreqd,
	documentformat,
	fixexchangerate,
	exchangerate,
	muldiv,
	currency,
	gstdeduction,
	orderstatusfail,
	consolidatedorder,
	creditedinvdate,
	job,
	serialisedflag,
	countersalesflag,
	nationality,
	deliveryterms,
	transactionnature,
	transportmode,
	processflag,
	jobsexistflag,
	alternatekey,
	lastoperator,
	hierarchyflag,
	depositflag,
	edisource,
	deliverynote,
	operator,
	linecomp,
	capturehh,
	capturemm,
	lastdelnote,
	timedelprtedhh,
	timedelprtedmm,
	timeinvprtedhh,
	timeinvprtedmm,
	datelastinvprt,
	salesperson2,
	salesperson3,
	salesperson4,
	commissionsales1,
	commissionsales2,
	commissionsales3,
	commissionsales4,
	timetakentoadd,
	timetakentochg,
	faxinvinbatch,
	interwhsale,
	sourcewarehouse,
	targetwarehouse,
	dispatchesmade,
	livedispexist,
	numdispatches,
	customername,
	shipaddress1,
	shipaddress2,
	shipaddress3,
	shipaddress3loc,
	shipaddress4,
	shipaddress5,
	shippostalcode,
	shiptogpslat,
	shiptogpslong,
	state,
	countyzip,
	extendedtaxcode,
	multishipcode,
	webcreated,
	quote,
	quoteversion,
	gtrreference,
	nonmerchflag,
	email,
	user1,
	companytaxno,
	tpmpickupflag,
	tpmevaluatedflag,
	standardcomment,
	detailstatus,
	salesordersource,
	salesordersrcdesc,
	languagecode,
	shippinglocation,
	includeinmrp,
	timestamp,
	quickquote,
	regimecode,
	portdispatch,
	pricegroup,
	pricegrouplevel,
	_rivery_river_id,
	_rivery_run_id,
	_rivery_last_update,
    etl_src_table_name,
 	source_system
from deduped
where row_nbr = 1