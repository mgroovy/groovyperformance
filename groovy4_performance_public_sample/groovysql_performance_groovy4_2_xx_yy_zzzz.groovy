package xx.yy.zzzz.groovysql.groovy.performance.groovy4

import xx.yy.zzzz.groovysql.sql.annotations.TableAnnotation
import xx.yy.zzzz.groovysql.sql.column.Column
import xx.yy.zzzz.groovysql.sql.column.Columns
import xx.yy.zzzz.groovysql.sql.column.mapping.MappingColumns
import xx.yy.zzzz.groovysql.sql.functions.CONCATCUR
import xx.yy.zzzz.groovysql.sql.global.Global
import xx.yy.zzzz.groovysql.sql.interfaces.DbLogger
import xx.yy.zzzz.groovysql.sql.procedures.LOG_MSG_Procedure
import xx.yy.zzzz.groovysql.sql.provider.table_short_name.TableShortNameGenerator
import xx.yy.zzzz.groovysql.sql.schema.Schema
import xx.yy.zzzz.groovysql.sql.sql_builder.SqlCommandsContainer
import xx.yy.zzzz.groovysql.sql.table.Table
import xx.yy.zzzz.groovysql.sql.table.TableGenericQuery
import xx.yy.zzzz.groovysql.sql.table.TableIntf
import xx.yy.zzzz.groovysql.sql.types.SqlTypes
import xx.yy.zzzz.groovyutil.groovy.GroovyFunctions
import xx.yy.zzzz.groovyutil.groovy.GroovyLogger
import xx.yy.zzzz.groovyutil.logging.TimingLogger
import xx.yy.zzzz.groovyutil.timing.PerformanceTimingProfiler
import xx.yy.zzzz.groovyutil.timing.Stopwatch
import groovy.transform.SelfType
import groovy.transform.TypeChecked

import java.lang.management.ManagementFactory

import static xx.yy.zzzz.groovymacro.groovy.stub.NameAndValueMacrosStubs.NW
import static xx.yy.zzzz.groovysql.groovy.performance.groovy4.EMAIL_Table.EMAIL
import static xx.yy.zzzz.groovysql.groovy.performance.groovy4.LDAP_Table.LDAP
import static xx.yy.zzzz.groovysql.groovy.performance.groovy4.LDAP_VAL_Table.LDAP_VAL
import static xx.yy.zzzz.groovysql.sql.global.Global.sqb

List<Map.Entry<String,Number>> resultList(PerformanceTimingProfiler ptp) {
	ptp.tagStopwatchMap.collect { new MapEntry(it.key,it.value.elapsedMilliseconds()) }.sort { it.key }
}


interface EntityIdTable {
	Column getEID_IDH()
	Column getTS_VON_IDH()
	Column getTS_BIS_IDH()
}

interface EntityTableIntf extends TableIntf {
	//EntityTableIntf reference() // 2022-04-29 "stubbed"
	Column getID_IDH()
	Column getEID_IDH()
	Column getIS_ACTIVE_IDH()
	Column getTS_VON_IDH()
	Column getTS_BIS_IDH()
}

@SelfType(Table)
trait CreatedUpdatedColumnsTable {
	final Column TS_CREATED_IDH = colThis('TS_CREATED_IDH', SqlTypes.TIMESTAMP_SYNC)
	final Column TS_UPDATED_IDH = colThis('TS_UPDATED_IDH', SqlTypes.TIMESTAMP_SYNC)
	final Column LAST_CHANGED_BY_IDH = colThis('LAST_CHANGED_BY_IDH', SqlTypes.VARCHAR2(32))
	final Column CREATED_BY_IDH = colThis('CREATED_BY_IDH', SqlTypes.VARCHAR2(32))
	
	Columns getCreatedUpdatedColumns() { TS_CREATED_IDH + CREATED_BY_IDH + TS_UPDATED_IDH + LAST_CHANGED_BY_IDH }
}

@TableAnnotation
@TypeChecked
abstract class IdhTable extends Table {}

@TableAnnotation
@TypeChecked
abstract class InsertIntoTable extends IdhTable {
	final Column PROVIDER_ID_IDH = colThis("PROVIDER_ID_IDH", SqlTypes.NUMBER_ID, false)
}

@TableAnnotation
@TypeChecked
abstract class EntityTable extends InsertIntoTable implements EntityTableIntf, EntityIdTable, CreatedUpdatedColumnsTable {
	private final static staticLog = GroovyLogger.get(EntityTable)
	static final boolean includeFpkColsInCompareColsQ = true
	static final boolean isNotNull = false
	
	@Lazy static final List<String> tableVariationPostfixes = [Table.currentTablePostfix, Table.errorTablePostfix, Table.tempTablePostfix, Table.historyTablePostfix]
	final Column ID_IDH = colThis("ID_IDH", SqlTypes.NUMBER_ID, isNotNull)
	final Column PREV_ID_IDH = colThis("PREV_ID_IDH", SqlTypes.NUMBER_ID, false)
	final Column EID_IDH = colThis("EID_IDH", SqlTypes.NUMBER_ID, isNotNull)
	final Column LEGACY_IDH1_ID = colThis("LEGACY_IDH1_ID", SqlTypes.NUMBER_ID, false)
	final Column TS_VON_IDH = colThis('TS_VON_IDH', SqlTypes.TIMESTAMP_SYNC, isNotNull)
	final Column TS_BIS_IDH = colThis('TS_BIS_IDH', SqlTypes.TIMESTAMP_SYNC)
	final Column SYNC_ID_IDH = colThis("SYNC_ID_IDH", SqlTypes.NUMBER_ID, isNotNull)
	final Column IS_ACTIVE_IDH = colThis("IS_ACTIVE_IDH", SqlTypes.BOOLEAN, isNotNull)
	final Column ENTRY_TYPE_IDH = colThis("ENTRY_TYPE_IDH", SqlTypes.NUMBER_ID, false)
	final Column CONSUMER_SYNC_ID_IDH = colThis("CONSUMER_SYNC_ID_IDH", SqlTypes.NUMBER_ID, isNotNull)
	final Column CONSUMER_PREV_ID_IDH = colThis("CONSUMER_PREV_ID_IDH", SqlTypes.NUMBER_ID, false)
}

@TableAnnotation
@TypeChecked
class EMAIL_Table extends EntityTable {
	final Column EMAILLL = colThis('EMAIL', LDAP_Table.it.MAIL)       // Collision-free Domino email address
	final Column TBNR = colThis('TBNR', SqlTypes.NUMBER(15))
	final Column NAME = colThis('NAME', LDAP_Table.it.DISPLAYNAME)       // Domino Name corrsponding to EMAIL col
	final Column MD_INDEX = colThis('MD_INDEX', LDAP_VAL.INDEX) // LDAP/Metadirectory email index in MAIL multivalue field
	final Column DNS = colThis('DNS', SqlTypes.VARCHAR2(1024)) // 2016-11-21: Size can be chosen independent of LDAP.DN Column, this is onyl a debug col, whose fill values get truncated to the max col size
	
	@Lazy static final EMAIL_Table it = new EMAIL_Table('EMAIL','em')
	static EMAIL_Table getEMAIL() { return it }
	EMAIL_Table getTempTable() { return it }
	@Override MappingColumns getPrimaryKeyColumnsOverride() { colsToMappingCols(EMAILLL.asColumns) }
}

@TableAnnotation
@TypeChecked
class LDAP_Table extends Table {
	static final String ldapToColumnNamePostfix = "_LDAP"
	final Column DN = colThis('DN', SqlTypes.VARCHAR2(256))
	final Column MAIL = colThis('MAIL', SqlTypes.VARCHAR2(256))
	final Column EMPLOYEEID = colThis('EMPLOYEEID', SqlTypes.VARCHAR2(64))
	final Column DISPLAYNAME = colThis('DISPLAYNAME', SqlTypes.VARCHAR2(256))
	final Column UID = colThis("UID$ldapToColumnNamePostfix", SqlTypes.VARCHAR2(256))
	final Column PID = colThis("PID", SqlTypes.NUMBER_ID)
	final Column PERSON_EID_IDH = colThis("PE_EID_IDH", SqlTypes.NUMBER_ID)
	final Column DN_O = colThis('DN_O', SqlTypes.VARCHAR2(64))
	final Column DN_OU = colThis('DN_OU', SqlTypes.VARCHAR2(64))
	final Column DN_DC = colThis('DN_DC', SqlTypes.VARCHAR2(64))
	final Column DN_CN = colThis('DN_CN', SqlTypes.VARCHAR2(256))	

	@Lazy static final LDAP_Table it = new LDAP_Table('LDAP','ld')
	static LDAP_Table getLDAP() { return it }
	LDAP_Table getTempTable() { return it }
	@Override MappingColumns getPrimaryKeyColumnsOverride() { colsToMappingCols(DN.asColumns) }
}

@TableAnnotation
@TypeChecked
class LDAP_VAL_Table extends Table {
	final Column DN = colThis(LDAP_Table.it.DN)
	final Column FIELD = colThis('FIELD', SqlTypes.VARCHAR2(64))
	final Column VALUE = colThis('VALUE', SqlTypes.VARCHAR2(256))
	final Column INDEX = colThis('INDX', SqlTypes.NUMBER(15))
	
	@Lazy static final LDAP_VAL_Table it = new LDAP_VAL_Table('LDAP_VAL','ldv')
	static LDAP_VAL_Table getLDAP_VAL() { return it }
	LDAP_VAL_Table getTempTable() { return it }
	@Override MappingColumns getPrimaryKeyColumnsOverride() { colsToMappingCols(DN.asColumns) }
}

protected def distinctEmailsFromLdapValRec() {
	return new TableGenericQuery("emailsFromLdapValRec") {
		final EMAILLL = colThis(EMAIL.EMAILLL)
		
		@Override
		GString theQuery() {
			final l =  Table.reference(LDAP)
			final lv = Table.reference(LDAP_VAL)
			"""
					select distinct ${EMAILLL.valExpression(sqb.lowerSql(sqb.trimSql(lv.VALUE)) ).asColumns}
					from $l, $lv
					where $lv.DN = $l.DN and (not $l.DN_OU in ('oebh', 'testbh')) and $lv.FIELD = 'MAIL'
				"""
		}
	}
}

// 2022-04-29: Stubbed!
@TypeChecked
GString emailEidLookupAllButTempAndCreateSql(emailExp) {
	"emailEidFn_STUB($emailExp)"
}



// 2022-04-29: Semi-stubbed!
@TypeChecked
void logInsertIntoTable(SqlCommandsContainer scc, final String what, final Table table) {
	final DbLogger dbLogger = LOG_MSG_Procedure.it
	dbLogger.logInfo("insert into: $what -> $table.nonRefParentTable")
}


/*
		2022-04-29: Performance of insertInto_EMAIL_ExistingLdap is 3s in Groovy 3.0.10 vs 6s in Groovy 4.0.1
*/
SqlCommandsContainer insertInto_EMAIL_ExistingLdap() {
	final scc = SqlCommandsContainer.create("insertInto_EMAIL_ExistingLdap")
	
	final destTable = EMAIL.tempTable
	
	final distinctLdapEmails = distinctEmailsFromLdapValRec()
	final ldpval1 = Table.reference(LDAP_VAL)
	final e = destTable
	final e0 = Table.reference(e)

	final metadirId = 1234567
	
	final cols  = (
		e.EID_IDH.valExpression(
			emailEidLookupAllButTempAndCreateSql(distinctLdapEmails.EMAILLL)
		) +
			
			e.PROVIDER_ID_IDH.val(metadirId) +
			e.EMAILLL.val(distinctLdapEmails.EMAILLL) +
			e.DNS.valExpression(CONCATCUR.it.callSql("select $ldpval1.DN from $ldpval1 where $ldpval1.FIELD = 'MAIL' and $ldpval1.VALUE = $distinctLdapEmails.EMAILLL order by $ldpval1.DN", '; ', e.DNS.sqlType.size))
	).sorted
	
	logInsertIntoTable(scc, "Existing LDAP", destTable)
	
	scc << """
			insert into $cols.insertIntoSql
			select $cols
			from $distinctLdapEmails
			where not exists (select 1 from $e0 where ${e0.EMAILLL.isEqualToSql(distinctLdapEmails.EMAILLL)})
		"""
	
	return scc
}



println "-"*20
println GroovyFunctions.osName
println GroovyFunctions.groovyVersion
println GroovyFunctions.javaVersion
println GroovyFunctions.javaRuntimeVersion
println GroovyFunctions.javaRuntimeName
println GroovyFunctions.javaVendorVersion
println GroovyFunctions.javaVmVersion
println GroovyFunctions.javaVmName
println ManagementFactory.getRuntimeMXBean().getInputArguments()
println "-"*20


final int nrLoops = 5
final int nrLoopsInner = 100
String sql = null

final dtList = []

for(int i0=0; i0 < nrLoops; i0++) {
	final Stopwatch sw = Stopwatch.createAndStart(true)

	for(int i=0; i < nrLoopsInner; i++) {
		sql = insertInto_EMAIL_ExistingLdap()
	}
	
	sw.stop()
	final dt = sw.elapsedMilliseconds/nrLoopsInner
	println "$i0) dt = $dt ms"
	dtList << dt
}

println NW(dtList)
println NW(dtList.tail())

final dt_average = dtList.tail().sum()/(nrLoops-1)

println "sql=$sql"

println "\ndt_average($nrLoops)=${dt_average} ms"