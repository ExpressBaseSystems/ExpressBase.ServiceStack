using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.ProductionDBManager;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace ExpressBase.ServiceStack.Services
{
    public class ProductionDBManagerServices : EbBaseService
    {
        public ProductionDBManagerServices(IEbConnectionFactory _dbf) : base(_dbf) { }
        Dictionary<string, string[]> dictTenant = new Dictionary<string, string[]>();
        Dictionary<string, Eb_FileDetails> dictInfra = new Dictionary<string, Eb_FileDetails>();

        public IDatabase GetTenantDB(string SolutionId)
        {
            IDatabase _ebconfactoryDatadb = null;
            EbConnectionFactory factory = new EbConnectionFactory(SolutionId, this.Redis, true);
            if (factory != null && factory.DataDB != null)
            {
                _ebconfactoryDatadb = factory.DataDB;
            }
            return _ebconfactoryDatadb;
        }

        public UpdateInfraWithSqlScriptsResponse Post(UpdateInfraWithSqlScriptsRequest request)
        {
            UpdateInfraWithSqlScriptsResponse resp = new UpdateInfraWithSqlScriptsResponse();
            SetFileMd5InfraReference();
            return resp;
        }

        public CheckChangesInFilesResponse Post(CheckChangesInFilesRequest request)
        {
            CheckChangesInFilesResponse resp = new CheckChangesInFilesResponse();
            CheckChangesInFilesResponse resp1 = new CheckChangesInFilesResponse();
            List<Eb_FileDetails> ChangesList = new List<Eb_FileDetails>();
            try
            {
                IDatabase _ebconfactoryDatadb = GetTenantDB(request.SolutionId);
                if (_ebconfactoryDatadb != null)
                {
                    GetFileScriptFromInfra(_ebconfactoryDatadb);
                    ChangesList = GetFileScriptFromTenant(_ebconfactoryDatadb);
                    if (request.IsUpdate)
                    {
                        resp.ModifiedDate = UpdateDBFunctionByDB(ChangesList, request.SolutionId);
                        request.IsUpdate = false;
                        resp1 = this.Post(request);
                        ChangesList = resp1.Changes;
                    }
                    resp.Changes = ChangesList;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return resp;
            }
            return resp;
        }

        public GetSolutionForIntegrityCheckResponse Post(GetSolutionForIntegrityCheckRequest request)
        {
            GetSolutionForIntegrityCheckResponse resp = new GetSolutionForIntegrityCheckResponse();
            List<Eb_Changes_Log> list = new List<Eb_Changes_Log>();
            string name = string.Empty;
            try
            {
                using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                {
                    con.Open();
                    string str = @"
                                SELECT * 
                                FROM eb_solutions 
                                WHERE eb_del = false 
                                AND tenant_id IN (select id from eb_tenants) 
                                AND isolution_id != ''";
                    EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
                    for (int i = 0; i < dt.Rows.Count; i++)
                    {
                        name = dt.Rows[i]["isolution_id"].ToString();
                        string str1 = string.Format(@"
                                   SELECT * 
                                   FROM eb_dbchangeslog
                                   WHERE solution_id = '{0}' ", name);
                        EbDataTable dt1 = InfraConnectionFactory.DataDB.DoQuery(str1);
                        if (dt1 != null && dt1.Rows.Count > 0)
                        {
                            string str2 = string.Format(@"
                                    SELECT d.modified_at, t.email , t.fullname 
                                    FROM eb_dbchangeslog as d, eb_tenants as t  
                                    WHERE d.solution_id = '{0}'
                                        AND  t.id = ( 
                                                        SELECT tenant_id 
                                                        FROM eb_solutions 
                                                        WHERE isolution_id = '{0}' 
                                                            AND eb_del = false)", name);
                            try
                            {
                                EbDataTable dt2 = InfraConnectionFactory.DataDB.DoQuery(str2);
                                if (dt2 != null && dt2.Rows.Count > 0)
                                {
                                    list.Add(new Eb_Changes_Log
                                    {
                                        Solution = name,
                                        DBName = dt1.Rows[0]["dbname"].ToString(),
                                        TenantName = dt2.Rows[0]["fullname"].ToString(),
                                        TenantEmail = dt2.Rows[0]["email"].ToString(),
                                        Last_Modified = DateTime.Parse(dt2.Rows[0][0].ToString()),
                                        Vendor = dt1.Rows[0]["vendor"].ToString()
                                    });
                                }
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                                continue;
                            }
                        }
                        else
                        {
                            try
                            {
                                IDatabase _ebconfactoryDatadb = GetTenantDB(name);
                                if (_ebconfactoryDatadb != null)
                                {
                                    string str2 = string.Format(@"
                                                SELECT t.email, t.fullname, s.date_created 
                                                FROM eb_solutions as s, eb_tenants as t 
                                                WHERE s.isolution_id = '{0}'
                                                    AND s.eb_del = false
                                                    AND s.tenant_id = t.id", name);
                                    try
                                    {
                                        EbDataTable dt2 = InfraConnectionFactory.DataDB.DoQuery(str2);
                                        if (dt2.Rows.Count > 0)
                                        {
                                            string str3 = string.Format(@"
                                                                INSERT INTO 
                                                                    eb_dbchangeslog (solution_id, dbname, vendor, modified_at)
                                                                VALUES ('{0}','{1}','{2}','{3}')", name, _ebconfactoryDatadb.DBName, _ebconfactoryDatadb.Vendor, dt2.Rows[0]["date_created"].ToString());
                                            DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str3);
                                            cmd.ExecuteNonQuery();
                                            list.Add(new Eb_Changes_Log
                                            {
                                                Solution = name,
                                                DBName = _ebconfactoryDatadb.DBName,
                                                TenantName = dt2.Rows[0]["fullname"].ToString(),
                                                TenantEmail = dt2.Rows[0]["email"].ToString(),
                                                Last_Modified = DateTime.Parse(dt2.Rows[0]["date_created"].ToString()),
                                                Vendor = _ebconfactoryDatadb.Vendor.ToString()
                                            });
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                                continue;
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            resp.ChangesLog = list;
            return resp;
        }

        void SetFileMd5InfraReference()
        {
            StringBuilder hash = new StringBuilder();
            string content = string.Empty;
            MD5CryptoServiceProvider md5provider = new MD5CryptoServiceProvider();
            string result = string.Empty;
            string file_name = string.Empty;
            string file_name_shrt = string.Empty;
            DBManagerType type = DBManagerType.Function;
            string[] func_create = SqlFiles.SQLSCRIPTS;

            foreach (string vendor in Enum.GetNames(typeof(DatabaseVendors)))
            {
                if (vendor == "PGSQL")
                {
                    string Urlstart = string.Format("ExpressBase.Common.sqlscripts.{0}.", vendor.ToLower());
                    foreach (string file in func_create)
                    {
                        string path = Urlstart + file;
                        var assembly = typeof(sqlscripts).Assembly;
                        using (Stream stream = assembly.GetManifestResourceStream(path))
                        {
                            if (stream != null)
                            {
                                using (StreamReader reader = new StreamReader(stream))
                                    result = reader.ReadToEnd();
                                if (result.Split("\n").Length > 1)
                                {
                                    if (file.Split(".")[1] == "functioncreate")
                                    {
                                        type = DBManagerType.Function;
                                        file_name = GetFileName(result, file, type);
                                        file_name_shrt = file_name.Split("(")[0];
                                        if (vendor == "PGSQL")
                                        {
                                            result = FormatFileStringPGSQL(result, type);
                                        }
                                        byte[] bytes = md5provider.ComputeHash(new UTF8Encoding().GetBytes(result));
                                        hash.Clear();
                                        for (int j = 0; j < bytes.Length; j++)
                                        {
                                            hash.Append(bytes[j].ToString("x2"));
                                        }
                                        content = hash.ToString();
                                    }
                                    else if (file.Split(".")[1] == "tablecreate")
                                    {
                                        type = DBManagerType.Table;
                                        file_name = GetFileName(result, file, type);
                                        file_name_shrt = file_name;
                                        using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                                        {
                                            con.Open();
                                            string str = String.Format(@"
                                                                        SELECT row_to_json(t)
                                                                        FROM 
	                                                                        (SELECT table_name,
		                                                                        (SELECT json_object(array_agg(column_name :: text ORDER BY column_name), array_agg(json_build_array(row_to_json(d))) :: text[])
      	                                                                         FROM 
				                                                                        (select 
						                                                                        C.data_type, C.column_default, TC.constraint_type, TC.constraint_name
					                                                                        from 
						                                                                        information_schema.columns C
					                                                                        left join 
						                                                                        information_schema.key_column_usage KC 
					                                                                        on 
						                                                                        (C.table_name = KC.table_name  AND C.column_name = KC.column_name)
					                                                                        left join 
						                                                                        information_schema.table_constraints TC
					                                                                        on
						                                                                        (KC.table_name = TC.table_name  AND KC.constraint_name = TC.constraint_name)	
					                                                                        where 
						                                                                        C.table_name = C1.table_name
					                                                                        order by  C.column_name
				                                                                        ) d
                                                                                 ) as fields,
		                                                                        (SELECT json_object( array_agg(indexname :: text ORDER BY indexname), array_agg(indexdef::text ORDER BY indexname)) 
		                                                                         FROM 
			                                                                        pg_indexes
		                                                                         WHERE tablename =  C1.table_name
		                                                                         GROUP BY tablename
		                                                                        )AS indexs 
                                                                            FROM information_schema.columns C1
	                                                                        WHERE table_name = '{0}' 
                                                                            GROUP BY table_name
	                                                                        ) t", file_name);
                                            EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
                                            if (dt != null && dt.Rows.Count > 0)
                                            {
                                                content = dt.Rows[0][0].ToString().Replace("'", "''").Replace("\"[", "[").Replace("]\"", "]").Replace("\\", "").Replace("\"char\"", "char");
                                            }
                                        }
                                    }
                                    using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                                    {
                                        con.Open();
                                        string str = string.Format(@"
                                                SELECT * 
                                                FROM eb_dbmd5 as d , eb_dbstructure as c
                                                WHERE d.filename = '{0}' 
                                                    AND d.eb_del = 'F' 
                                                    AND c.vendor = '{1}'
                                                    AND d.change_id = c.id", file_name, vendor);
                                        EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
                                        if (dt != null && dt.Rows.Count > 0)
                                        {
                                            string str1 = string.Format(@"
                                            SELECT * 
                                            FROM eb_dbmd5 as d , eb_dbstructure as c
                                            WHERE  d.change_id = {0}
                                                AND c.id = {0}
                                                AND d.eb_del = 'F'
                                                AND d.contents <> '{1}'", dt.Rows[0]["change_id"].ToString(), content);
                                            EbDataTable dt1 = InfraConnectionFactory.DataDB.DoQuery(str1);
                                            if (dt1 != null && dt1.Rows.Count > 0)
                                            {
                                                string str2 = string.Format(@"
                                                    UPDATE eb_dbmd5 
                                                    SET eb_del = 'T'
                                                    WHERE filename = '{0}' 
                                                        AND eb_del = 'F'
                                                        AND change_id = '{1}'", file_name, dt1.Rows[0]["change_id"].ToString());
                                                DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str2);
                                                cmd.ExecuteNonQuery();

                                                string str3 = string.Format(@"
                                                    INSERT INTO 
                                                        eb_dbmd5 (change_id, filename, contents, eb_del)
                                                    VALUES ('{0}','{1}','{2}','F')", dt1.Rows[0]["change_id"], file_name, content);
                                                DbCommand cmd1 = InfraConnectionFactory.DataDB.GetNewCommand(con, str3);
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                        else
                                        {
                                            string str1 = string.Format(@"
                                            INSERT INTO 
                                                eb_dbstructure (filename, filepath, vendor, type)
                                            VALUES ('{0}','{1}','{2}','{3}')", file_name_shrt, file, vendor, (int)type);
                                            DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str1);
                                            cmd.ExecuteNonQuery();

                                            string str2 = string.Format(@"
                                                INSERT INTO 
                                                    eb_dbmd5 (change_id, filename, contents, eb_del)
                                                VALUES ((SELECT id 
                                                FROM eb_dbstructure
                                                WHERE filename = '{0}'
                                                    AND vendor = '{1}'),'{2}','{3}','F')", file_name_shrt, vendor, file_name, content);
                                            DbCommand cmd1 = InfraConnectionFactory.DataDB.GetNewCommand(con, str2);
                                            cmd1.ExecuteNonQuery();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        void GetFileScriptFromInfra(IDatabase _ebconfactoryDatadb)
        {
            string vendor = _ebconfactoryDatadb.Vendor.ToString();
            dictInfra.Clear();
            string str = string.Format(@"
                        SELECT d.change_id, d.filename, d.contents, c.filepath, c.type
                        FROM eb_dbmd5 as d, eb_dbstructure as c
                        WHERE c.vendor = '{0}'
                        AND d.eb_del = 'F'
                        AND c.id = d.change_id
                        AND (c.type = '0'
                        OR c.type='1')
                        ORDER BY c.type", vendor);
            EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
            if (dt != null && dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    dictInfra.Add(dt.Rows[i]["filename"].ToString(), new Eb_FileDetails
                    {
                        Id = dt.Rows[i]["change_id"].ToString(),
                        Vendor = vendor,
                        FilePath = dt.Rows[i]["filepath"].ToString(),
                        Content = dt.Rows[i]["contents"].ToString().Trim(),
                        Type = (DBManagerType)Convert.ToInt32(dt.Rows[i]["type"])
                    });
                }
            }
        }

        List<Eb_FileDetails> GetFileScriptFromTenant(IDatabase _ebconfactoryDatadb)
        {
            string str = string.Empty;
            StringBuilder hash = new StringBuilder();
            MD5CryptoServiceProvider md5provider = new MD5CryptoServiceProvider();
            string result = string.Empty;
            string file_name;
            string vendor = _ebconfactoryDatadb.Vendor.ToString();
            dictTenant.Clear();
            if (_ebconfactoryDatadb.Vendor == DatabaseVendors.PGSQL)
            {
                str = @"
                        SELECT pg_get_functiondef(oid)::text, proname 
                        FROM pg_proc 
                        WHERE proname 
                        IN 
                            (SELECT routine_name 
                            FROM information_schema.routines 
                            WHERE routine_type = 'FUNCTION' 
                                AND specific_schema = 'public')";
                EbDataTable dt = _ebconfactoryDatadb.DoQuery(str);
                if (dt != null && dt.Rows.Count > 0)
                {
                    for (int i = 0; i < dt.Rows.Count; i++)
                    {
                        result = dt.Rows[i][0].ToString();
                        file_name = GetFileName(result, dt.Rows[i][1].ToString(), DBManagerType.Function);
                        result = FormatDBStringPGSQL(result, DBManagerType.Function);
                        byte[] bytes = md5provider.ComputeHash(new UTF8Encoding().GetBytes(result));
                        hash.Clear();
                        for (int j = 0; j < bytes.Length; j++)
                        {
                            hash.Append(bytes[j].ToString("x2"));
                        }
                        dictTenant.Add(file_name, new[] { vendor, hash.ToString(), DBManagerType.Function.ToString() });
                    }
                }

                str = @"
                        SELECT row_to_json(t)
                        FROM 
	                        (SELECT table_name,
		                        (SELECT json_object(array_agg(column_name :: text ORDER BY column_name), array_agg(json_build_array(row_to_json(d))) :: text[])
      	                         FROM 
				                        (select 
						                        C.data_type, C.column_default, TC.constraint_type, TC.constraint_name 
					                        from 
						                        information_schema.columns C
					                        left join 
						                        information_schema.key_column_usage KC 
					                        on 
						                        (C.table_name = KC.table_name  AND C.column_name = KC.column_name)
					                        left join 
						                        information_schema.table_constraints TC
					                        on
						                        (KC.table_name = TC.table_name  AND KC.constraint_name = TC.constraint_name)	
					                        where 
						                        C.table_name = C1.table_name
					                        order by  C.column_name
				                        ) d
                                 ) as fields,
		                        (SELECT json_object( array_agg(indexname :: text ORDER BY indexname), array_agg(indexdef::text ORDER BY indexname)) 
		                         FROM 
			                        pg_indexes
		                         WHERE tablename =  C1.table_name
		                         GROUP BY tablename
		                        )AS indexs 
                            FROM information_schema.columns C1
	                        WHERE table_name LIKE 'eb_%' 
                            GROUP BY table_name
	                        ) t";
                EbDataTable dt1 = _ebconfactoryDatadb.DoQuery(str);
                if (dt1 != null && dt1.Rows.Count > 0)
                {
                    for (int i = 0; i < dt1.Rows.Count; i++)
                    {
                        result = dt1.Rows[i][0].ToString().Replace("\"[", "[").Replace("]\"", "]").Replace("\\", "").Replace("\"char\"", "char");
                        file_name = GetFileName(result, null, DBManagerType.Table);
                        dictTenant.Add(file_name, new[] { vendor, result, DBManagerType.Table.ToString() });
                    }
                }
            }
            return CompareScripts(_ebconfactoryDatadb);
        }
        
        List<Eb_FileDetails> CompareScripts(IDatabase _ebconfactoryDatadb)                                                                                                                
        {
            List<Eb_FileDetails> ChangesList = new List<Eb_FileDetails>();

            foreach (KeyValuePair<string, Eb_FileDetails> _infraitem in dictInfra)
            {
                if (dictTenant.TryGetValue(_infraitem.Key, out string[] _tenantitem))
                {
                    if ((_tenantitem != null) && (_tenantitem[1] != _infraitem.Value.Content))
                    {
                        if(_infraitem.Value.Type == DBManagerType.Table)
                        {
                            int f = 0;
                            Eb_TableFieldChangesList infra_table_details = JsonConvert.DeserializeObject<Eb_TableFieldChangesList>(_infraitem.Value.Content);
                            Eb_TableFieldChangesList tenant_table_details = JsonConvert.DeserializeObject<Eb_TableFieldChangesList>(_tenantitem[1]);
                            foreach (KeyValuePair<string, List<Eb_TableFieldChanges>> _infratableitem in infra_table_details.Fields)
                            {
                                if (!tenant_table_details.Fields.TryGetValue(_infratableitem.Key, out List<Eb_TableFieldChanges> _tenanttableitem))
                                {
                                    f = 1;
                                }
                                else
                                {
                                    if (_infratableitem.Value[0].Data_type != _tenanttableitem[0].Data_type)
                                        f = 1;
                                    if (_infratableitem.Value[0].Column_default != _tenanttableitem[0].Column_default)
                                        f = 1;
                                    if (_infratableitem.Value[0].Constraint_name != _tenanttableitem[0].Constraint_name)
                                        f = 1;
                                    if (_infratableitem.Value[0].Constraint_type != _tenanttableitem[0].Constraint_type)
                                        f = 1;
                                }
                            }
                            foreach (KeyValuePair<string, string> _infratableitem in infra_table_details.Indexs)
                            {
                                if (!tenant_table_details.Indexs.TryGetValue(_infratableitem.Key, out string _tenanttableitem))
                                {
                                    f = 1;
                                }
                                else
                                {
                                    if (_infratableitem.Value != _tenanttableitem)
                                        f = 1;
                                }
                            }
                            if (f==1)
                            {
                                ChangesList.Add(new Eb_FileDetails
                                {
                                    Id = _infraitem.Value.Id,
                                    FileHeader = _infraitem.Key,
                                    FilePath = _infraitem.Value.FilePath,
                                    Vendor = _infraitem.Value.Vendor,
                                    Content = _infraitem.Value.Content,
                                    Type = _infraitem.Value.Type,
                                    FileType = Enum.GetName(typeof(DBManagerType), _infraitem.Value.Type),
                                    NewItem = false
                                });
                                f = 0;
                            }
                        }
                        else
                        {
                            ChangesList.Add(new Eb_FileDetails
                            {
                                Id = _infraitem.Value.Id,
                                FileHeader = _infraitem.Key,
                                FilePath = _infraitem.Value.FilePath,
                                Vendor = _infraitem.Value.Vendor,
                                Content = _infraitem.Value.Content,
                                Type = _infraitem.Value.Type,
                                FileType = Enum.GetName(typeof(DBManagerType), _infraitem.Value.Type),
                                NewItem = false
                            });
                        }
                    }
                }
                else
                {
                    ChangesList.Add(new Eb_FileDetails
                    {
                        Id = _infraitem.Value.Id,
                        FileHeader = _infraitem.Key,
                        FilePath = _infraitem.Value.FilePath,
                        Vendor = _infraitem.Value.Vendor,
                        Content = _infraitem.Value.Content,
                        Type = _infraitem.Value.Type,
                        FileType = Enum.GetName(typeof(DBManagerType), _infraitem.Value.Type),
                        NewItem = true
                    });
                }
            }
            return ChangesList;
        }
        
        void UpdateDB(List<Eb_FileDetails> ChangesList, IDatabase _ebconfactoryDatadb)
        {
            string result = string.Empty;
            for (int i = 0; i < ChangesList.Count; i++)
            {
                if (ChangesList[i].Type == DBManagerType.Function)
                {
                    string Urlstart = string.Format("ExpressBase.Common.sqlscripts.{0}.", ChangesList[i].Vendor.ToLower());
                    string path = Urlstart + ChangesList[i].FilePath;
                    var assembly = typeof(sqlscripts).Assembly;
                    using (Stream stream = assembly.GetManifestResourceStream(path))
                    {
                        if (stream != null)
                        {
                            using (StreamReader reader = new StreamReader(stream))
                                result = reader.ReadToEnd();
                            string fun = GetFuncDef(result, ChangesList[i].FileHeader);
                            if (!ChangesList[i].NewItem)
                            {
                                using (DbConnection con = _ebconfactoryDatadb.GetNewConnection())
                                {
                                    con.Open();
                                    DbCommand cmd = _ebconfactoryDatadb.GetNewCommand(con, fun);
                                    int y = cmd.ExecuteNonQuery();
                                    con.Close();
                                }
                            }
                            using (DbConnection con1 = _ebconfactoryDatadb.GetNewConnection())
                            {
                                con1.Open();
                                DbCommand cmd1 = _ebconfactoryDatadb.GetNewCommand(con1, result);
                                int x = cmd1.ExecuteNonQuery();
                                con1.Close();
                            }
                        }
                    }
                }
                else if (ChangesList[i].Type == DBManagerType.Table)
                {
                    if (ChangesList[i].NewItem)
                    {
                        string Urlstart = string.Format("ExpressBase.Common.sqlscripts.{0}.", ChangesList[i].Vendor.ToLower());
                        string path = Urlstart + ChangesList[i].FilePath;
                        var assembly = typeof(sqlscripts).Assembly;
                        using (Stream stream = assembly.GetManifestResourceStream(path))
                        {
                            if (stream != null)
                            {
                                using (StreamReader reader = new StreamReader(stream))
                                    result = reader.ReadToEnd();
                                using (DbConnection con1 = _ebconfactoryDatadb.GetNewConnection())
                                {
                                    con1.Open();
                                    DbCommand cmd1 = _ebconfactoryDatadb.GetNewCommand(con1, result);
                                    int x = cmd1.ExecuteNonQuery();
                                    con1.Close();
                                }
                            }
                        }
                    }
                    else
                    {
                        Eb_TableFieldChangesList infra_table_details = JsonConvert.DeserializeObject<Eb_TableFieldChangesList>(ChangesList[i].Content);
                        Eb_TableFieldChangesList tenant_table_details = null;
                        string changes = String.Empty;
                        string str = string.Format(@"
                                                    SELECT row_to_json(t)
                                                    FROM 
	                                                    (SELECT table_name,
		                                                    (SELECT json_object(array_agg(column_name :: text ORDER BY column_name), array_agg(json_build_array(row_to_json(d))) :: text[])
      	                                                     FROM 
				                                                    (select 
						                                                    C.data_type, C.column_default, TC.constraint_type, TC.constraint_name 
					                                                    from 
						                                                    information_schema.columns C
					                                                    left join 
						                                                    information_schema.key_column_usage KC 
					                                                    on 
						                                                    (C.table_name = KC.table_name  AND C.column_name = KC.column_name)
					                                                    left join 
						                                                    information_schema.table_constraints TC
					                                                    on
						                                                    (KC.table_name = TC.table_name  AND KC.constraint_name = TC.constraint_name)	
					                                                    where 
						                                                    C.table_name = C1.table_name
					                                                    order by  C.column_name
				                                                    ) d
                                                             ) as fields,
		                                                    (SELECT json_object( array_agg(indexname :: text ORDER BY indexname), array_agg(indexdef::text ORDER BY indexname)) 
		                                                     FROM 
			                                                    pg_indexes
		                                                     WHERE tablename =  C1.table_name
		                                                     GROUP BY tablename
		                                                    )AS indexs 
                                                        FROM information_schema.columns C1
	                                                    WHERE table_name = '{0}' 
                                                        GROUP BY table_name
	                                                    ) t", ChangesList[i].FileHeader);
                        EbDataTable dt = _ebconfactoryDatadb.DoQuery(str);
                        if (dt != null && dt.Rows.Count > 0)
                        {
                            result = dt.Rows[0][0].ToString().Replace("\"[", "[").Replace("]\"", "]").Replace("\\", "").Replace("\"char\"", "char");
                        }
                        tenant_table_details = JsonConvert.DeserializeObject<Eb_TableFieldChangesList>(result);

                        foreach (KeyValuePair<string, List<Eb_TableFieldChanges>> _infratableitem in infra_table_details.Fields)
                        {
                            if (!tenant_table_details.Fields.TryGetValue(_infratableitem.Key, out List<Eb_TableFieldChanges> _tenanttableitem))
                            {
                                changes = changes + string.Format(@"
                                                        ALTER TABLE {0} 
                                                        ADD COLUMN {1} {2};
                                                        ", infra_table_details.Table_name, _infratableitem.Key, _infratableitem.Value[0].Data_type);
                                if(_infratableitem.Value[0].Column_default != null)
                                {
                                    changes = changes + string.Format(@"
                                                                        ALTER TABLE {0}
                                                                        ALTER COLUMN {1}
                                                                        SET DEFAULT {2};
                                                                        ALTER TABLE {0} 
                                                                        ALTER COLUMN {1} 
                                                                        SET NOT NULL;
                                                                        ", infra_table_details.Table_name, _infratableitem.Key, _infratableitem.Value[0].Column_default);
                                }
                                if (_infratableitem.Value[0].Constraint_type == "UNIQUE")
                                {
                                    changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    ADD CONSTRAINT {1} UNIQUE ({2});
                                                    ", infra_table_details.Table_name, _infratableitem.Value[0].Constraint_name, _infratableitem.Key);
                                }
                                else if (_infratableitem.Value[0].Constraint_type == "PRIMARY KEY")
                                {
                                    changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    ADD PRIMARY KEY ({1});
                                                    ", infra_table_details.Table_name,  _infratableitem.Key);
                                }
                                else if(_infratableitem.Value[0].Constraint_type == "CHECK")
                                {
                                    string str1 = string.Format(@"
                                                                        SELECT consrc
                                                                        FROM pg_catalog.pg_constraint con
	                                                                        INNER JOIN pg_catalog.pg_class rel
	                                                                        ON rel.oid = con.conrelid
	                                                                        INNER JOIN pg_catalog.pg_namespace nsp
	                                                                        ON nsp.oid = connamespace
                                                                        WHERE 
                                                                            conname = '{0}';
                                                                    ", _infratableitem.Value[0].Constraint_name);
                                    EbDataTable dt1 = InfraConnectionFactory.DataDB.DoQuery(str1);
                                    if (dt1 != null && dt1.Rows.Count > 0)
                                    {
                                        string expression = dt1.Rows[0][0].ToString();
                                        changes = changes + string.Format(@"
                                                                               ALTER TABLE {0} 
                                                                               ADD CONSTRAINT {1} CHECK {2};
                                                                               ", infra_table_details.Table_name, _infratableitem.Value[0].Constraint_name, _infratableitem.Key, expression);
                                    }
                                }

                                //Check Primary key already exist or not
                                //Foreign Key 
                            }
                            else
                            {
                                if (_infratableitem.Value[0].Data_type != _tenanttableitem[0].Data_type)
                                {
                                    changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    ALTER COLUMN {1} TYPE {2};", infra_table_details.Table_name, _infratableitem.Key, _infratableitem.Value[0].Data_type);
                                }
                                if (_infratableitem.Value[0].Constraint_type != _tenanttableitem[0].Constraint_type)
                                {
                                    if (_infratableitem.Value[0].Constraint_type == null)
                                    {
                                        changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    DROP CONSTRAINT {1};
                                                    ", infra_table_details.Table_name, _tenanttableitem[0].Constraint_name);
                                    }
                                    else if (_infratableitem.Value[0].Constraint_type == "UNIQUE")
                                    {
                                        changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    ADD CONSTRAINT {1} UNIQUE ({2});
                                                    ", infra_table_details.Table_name, _infratableitem.Value[0].Constraint_name, _infratableitem.Key);
                                    }
                                    else if (_infratableitem.Value[0].Constraint_type == "CHECK")
                                    {
                                        string str1 = string.Format(@"
                                                                        SELECT consrc
                                                                        FROM pg_catalog.pg_constraint con
	                                                                        INNER JOIN pg_catalog.pg_class rel
	                                                                        ON rel.oid = con.conrelid
	                                                                        INNER JOIN pg_catalog.pg_namespace nsp
	                                                                        ON nsp.oid = connamespace
                                                                        WHERE 
                                                                            conname = '{0}';
                                                                    ", _infratableitem.Value[0].Constraint_name);
                                        EbDataTable dt1 = _ebconfactoryDatadb.DoQuery(str1);
                                        if (dt1 != null && dt1.Rows.Count > 0)
                                        {
                                            string expression = dt1.Rows[0][0].ToString();
                                            changes = changes + string.Format(@"
                                                                               ALTER TABLE {0} 
                                                                               ADD CONSTRAINT {1} CHECK {2};
                                                                               ", infra_table_details.Table_name, _infratableitem.Value[0].Constraint_name, _infratableitem.Key, expression);
                                        }
                                    }
                                }
                                if (_infratableitem.Value[0].Column_default != _tenanttableitem[0].Column_default)
                                {
                                    if (_infratableitem.Value[0].Column_default == null)
                                    {
                                        changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    ALTER COLUMN {1} DROP DEFAULT;
                                                    ", infra_table_details.Table_name, _infratableitem.Key);
                                    }
                                    else
                                    {
                                        if (_infratableitem.Value[0].Column_default.Contains("nextval"))
                                        {
                                            string infra_seq_name = _infratableitem.Value[0].Column_default.Split("'")[1];
                                            string tenant_seq_name = _tenanttableitem[0].Column_default.Split("'")[1];
                                            string str1 = string.Format(@"
                                                    SELECT COUNT(*) 
                                                    FROM information_schema.sequences 
                                                    WHERE sequence_name='{0}'", infra_seq_name);
                                            EbDataTable dt1 = _ebconfactoryDatadb.DoQuery(str1);
                                            if (dt1 != null)
                                            {
                                                if (dt1.Rows.Count == 0)
                                                {
                                                    changes = changes + string.Format(@"
                                                                                        CREATE SEQUENCE IF NOT EXISTS {0};
                                                                                        SELECT setval('{0}', (SELECT max({1})+1 FROM {2}), false);
                                                                                        ALTER TABLE {2} ALTER COLUMN {1} SET DEFAULT nextval('{0}');
                                                                                        ", infra_seq_name, _infratableitem.Key, infra_table_details.Table_name);
                                                }
                                                else if (dt1.Rows.Count > 0)
                                                {
                                                    changes = changes + string.Format(@"
                                                                                        DROP SEQUENCE {0};
                                                                                        ALTER SEQUENCE {1} RENAME TO {0};
                                                                                        ", infra_seq_name,tenant_seq_name);
                                                }
                                            }
                                        }
                                        else
                                        {
                                            changes = changes + string.Format(@"
                                                   ALTER TABLE {0} ALTER COLUMN {1} SET DEFAULT {2};
                                                    ", infra_table_details.Table_name, _infratableitem.Key, _infratableitem.Value[0].Column_default);
                                        }
                                    }
                                }
                            }

                            //string str1 = string.Format(@"
                            //                            ALTER TABLE {0} DROP COLUMN {1}", infra_table_dict.Table_name, _infratableitem.Key);

                        }

                        foreach (KeyValuePair<string, string> _infratableitem in infra_table_details.Indexs)
                        {
                            if (!tenant_table_details.Indexs.TryGetValue(_infratableitem.Key, out string _tenanttableitem))
                            {
                                changes = changes + _infratableitem.Value + ";";
                            }
                            else
                            {
                                if (_infratableitem.Value != _tenanttableitem)
                                {
                                    changes = changes + string.Format(@"DROP INDEX {0};", _infratableitem.Key) + _infratableitem.Value + ";";
                                }
                            }

                        }

                        using (DbConnection con1 = _ebconfactoryDatadb.GetNewConnection())
                        {
                            con1.Open();
                            DbCommand cmd1 = _ebconfactoryDatadb.GetNewCommand(con1, changes);
                            int x = cmd1.ExecuteNonQuery();
                            con1.Close();
                            changes = string.Empty;
                        }
                    }
                }
            }
        }

        string UpdateDBFunctionByDB(List<Eb_FileDetails> ChangesList, string Solution)
        {
            DateTime modified_date = DateTime.Now;
            UpdateDBFunctionByDBResponse resp = new UpdateDBFunctionByDBResponse();
            IDatabase _ebconfactoryDatadb = GetTenantDB(Solution);
            UpdateDB(ChangesList, _ebconfactoryDatadb);
            using (DbConnection con = InfraConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                string str1 = string.Format(@"
                                              UPDATE eb_dbchangeslog 
                                              SET modified_at = NOW()
                                              WHERE solution_id = '{0}'", Solution);
                DbCommand cmd2 = InfraConnectionFactory.DataDB.GetNewCommand(con, str1);
                cmd2.ExecuteNonQuery();
            }
            return modified_date.ToString();
        }

        string GetFuncDef(string str, string filename)
        {
            string[] split = str.Split("\r\n\r\n");
            if (split.Length > 1)
            {
                str = split[1];
                str = str.Replace("-- ", "").Replace(";", "");
            }
            else
            {
                str = "DROP FUNCTION " + filename;
            }
            return str;
        }

        string GetFileName(string str, string file, DBManagerType type)
        {
            string[] fname;
            string res = string.Empty;
            if (type == DBManagerType.Function)
            {
                Regex regex = new Regex(@".*?\(.*?\)");
                str = str.Replace("\r", "").Replace("\n", "").Replace("\t", "").Replace("  ", "");
                MatchCollection matches = regex.Matches(str);
                if (matches.Count > 1)
                {
                    res = matches[0].Value.Contains("CREATE") ? matches[0].Value.Split(".")[1] : matches[2].Value.Split(".")[1];
                    res = res.Replace(" DEFAULT NULL::text", "").Replace(" DEFAULT NULL::integer", "").Replace(" DEFAULT 0", "");
                }
                else
                {
                    fname = file.Split(".");
                    res = fname.Length > 1 ? fname[2] + "()" : file + "()";
                }
                res = res.Replace(", ", ",").Trim();
            }
            else if (type == DBManagerType.Table && file != null)
            {
                int x = file.Split(".").Length;
                res = str.Split(" ").Length > 1 ? str.Split(" ")[2] : file.Split(".")[file.Split(".").Length - 2];
                res = res.Remove(0, 7).Replace("\r", "").Replace("\n", "").Replace("--", "");
            }
            else if (type == DBManagerType.Table && file == null)
            {
                res = str.Split("\"")[3];
            }
            return res;
        }

        string FormatDBStringPGSQL(string str, DBManagerType type)
        {
            if (type == DBManagerType.Function)
            {
                str = str.Replace(" ", "").Replace("\r", "").Replace("\t", "").Replace("\n", "").Trim();
            }
            else if (type == DBManagerType.Table)
            {
                Regex regex = new Regex(@"integer DEFAULT nextval\(.*?\) NOT NULL");
                MatchCollection matches = regex.Matches(str);
                str = str.Replace(matches[0].ToString(), "serial").Replace("  NULL", "");
                str = str.Replace(" ", "").Replace("\r", "").Replace("\t", "").Replace("\n", "").Trim();
                str = str + ")";
            }
            return str;
        }

        string FormatFileStringPGSQL(string str, DBManagerType type)
        {
            if (type == DBManagerType.Function)
            {
                str = str.Replace("$BODY$", "$function$");
                string[] split = str.Split("$function$");
                if (split.Length == 3)
                {
                    string[] split1 = split[0].Split("\r\n\r\n");
                    str = split1[2] + split[1] + "$function$";
                    str = str.Replace(" ", "").Replace("\r", "").Replace("\t", "").Replace("\n", "").Replace("'plpgsql'", "plpgsqlAS$function$").Replace("plpgsqlAS$function$AS", "plpgsqlAS$function$");
                }
                else if (split.Length == 1)
                {
                    str = "";
                }
            }
            else if (type == DBManagerType.Table)
            {
                string s = str;
                str = s;
                str = str.Split("\r\n\r\n")[2];
                str = str.Replace(" ", "").Replace("\r", "").Replace("\t", "").Replace("\n", "");
                if (str.IndexOf(",CONSTRAINT") > 0)
                    str = str.Remove(str.IndexOf(",CONSTRAINT"));
                str = str + ")";
            }
            return str;
        }

        string GetFuncDef(string str)
        {
            string[] split = str.Split("\r\n\r\n");
            if (split.Length > 1)
            {
                str = split[1];
                str = str.Replace("-- ", "").Replace(";", "");
            }
            else
            {
            }
            return str;
        }
    }
}


