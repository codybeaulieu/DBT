with src as (
    select 
        *   
         ,'{{source('cmlsyspro','arcustomer')}}' as etl_src_table_name
         ,'CMLSYSPRO'                                as source_system
    from {{source('cmlsyspro','arcustomer')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by customer,name order by _rivery_last_update desc) row_nbr
    from src
)
select
	customer,
	name,
	shortname,
	exemptfinchg,
	mainthistory,
	customertype,
	masteraccount,
	storenumber,
	prtmasteradd,
	creditstatus,
	creditlimit,
	invoicecount,
	salesperson,
	salesperson1,
	salesperson2,
	salesperson3,
	pricecode,
	customerclass,
	branch,
	termscode,
	invdisccode,
	balancetype,
	area,
	linedisccode,
	taxstatus,
	taxexemptnumber,
	specialinstrs,
	pricecategorytable,
	datelastsale,
	datelastpay,
	outstordval,
	numoutstord,
	telephone,
	contact,
	addtelephone,
	fax,
	telex,
	telephoneextn,
	currency,
	userfield1,
	userfield2,
	gstexemptflag,
	gstexemptnum,
	gstlevel,
	detailmovereqd,
	interfaceflag,
	contractprcreqd,
	buyinggroup1,
	buyinggroup2,
	buyinggroup3,
	buyinggroup4,
	buyinggroup5,
	statementreqd,
	backordreqd,
	shippinginstrs,
	shippinginstrscod,
	statecode,
	datecustadded,
	stockinterchange,
	maintlastprcpaid,
	ibtcustomer,
	sodefaultdoc,
	counterslsonly,
	paymentstatus,
	nationality,
	highestbalance,
	customeronhold,
	invcommentcode,
	edisendercode,
	relordosvalue,
	ediflag,
	sodefaulttype,
	email,
	applyorddisc,
	applylinedisc,
	faxinvoices,
	faxstatements,
	highinvdays,
	highinv,
	docfax,
	docfaxcontact,
	soldtoaddr1,
	soldtoaddr2,
	soldtoaddr3,
	soldtoaddr3loc,
	soldtoaddr4,
	soldtoaddr5,
	soldpostalcode,
	soldtogpslat,
	soldtogpslong,
	shiptoaddr1,
	shiptoaddr2,
	shiptoaddr3,
	shiptoaddr3loc,
	shiptoaddr4,
	shiptoaddr5,
	shippostalcode,
	shiptogpslat,
	shiptogpslong,
	state,
	countyzip,
	city,
	state1,
	countyzip1,
	city1,
	defaultordtype,
	ponumbermandatory,
	creditcheckflag,
	companytaxnumber,
	deliveryterms,
	transactionnature,
	deliverytermsc,
	transactionnaturec,
	routecode,
	faxquotes,
	routedistance,
	tpmcustomerflag,
	saleswarehouse,
	tpmpricingflag,
	arstatementno,
	tpmcreditcheck,
	wholeordershipflag,
	minimumordervalue,
	minimumorderchgcod,
	ukvatflag,
	ukcurrency,
	languagecode,
	shippinglocation,
	altmethodflag,
	salesallowed,
	unapppayallowed,
	paymentsallowed,
	quotesallowed,
	crnotesallowed,
	drnotesallowed,
	queryallowed,
	timestamp,
	dunninggroup,
	dunningfax,
	dunningemail,
	primarycustomer,
	pricegroup,
	thirdpartytaxinteg,
	pickmarshalling,
	_rivery_river_id,
	_rivery_run_id,
	_rivery_last_update,
    etl_src_table_name,
 	source_system
from deduped
where row_nbr = 1
