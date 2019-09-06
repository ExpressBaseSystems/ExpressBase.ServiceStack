using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Data;
using ExpressBase.Common.ProductionDBManager;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace ExpressBase.ServiceStack.Services
{
    public class ProductionDBManagerServices : EbBaseService
    {
        public ProductionDBManagerServices(IEbConnectionFactory _dbf) : base(_dbf) { }
        private List<string> _solnnames = null;
        List<string> SolNames
        {
            get
            {
                if (_solnnames == null)
                {
                    EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery("SELECT isolution_id FROM eb_solutions where solution_id is not null");
                    if (dt != null && dt.Rows.Count > 0)
                    {
                        _solnnames = new List<string>();
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            _solnnames.Add(dt.Rows[i][0].ToString());
                        }
                    }
                }
                return _solnnames;
            }
        }

        Dictionary<string, string[]> dictTenant = new Dictionary<string, string[]>();
        Dictionary<string, Eb_FileChanges> dictInfra = new Dictionary<string, Eb_FileChanges>();
        Dictionary<string, List<Eb_FileChanges>> dictDBFunctionChanges = new Dictionary<string, List<Eb_FileChanges>>();

        public CheckChangesInFunctionResponse Post(CheckChangesInFunctionRequest request)
        {
            CheckChangesInFunctionResponse resp = new CheckChangesInFunctionResponse();
            try
            {
                EbConnectionsConfig _solutionConnections = EbConnectionsConfigProvider.GetDataCenterConnections();
                _solutionConnections.DataDbConfig.DatabaseName = request.SolutionId;
                IDatabase _ebconfactoryDatadb = new EbConnectionFactory(_solutionConnections, request.SolutionId).DataDB;
                //SetFuncMd5InfraReference(request.SolutionId, _ebconfactoryDatadb);
                GetFuncScriptFromInfra();
                GetFuncScriptFromTenant(request.SolutionId);
                resp.Changes = dictDBFunctionChanges;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return resp;
        }

        public DBIntegrityCheckResponse Post(DBIntegrityCheckRequest request)
        {
            DBIntegrityCheckResponse resp = new DBIntegrityCheckResponse();
            List<Eb_Changes_Log> list = new List<Eb_Changes_Log>();
            EbConnectionFactory _ebConnectionFactory;
            foreach (string name in SolNames)
                if (name != string.Empty)
                {
                    try
                    {
                        _ebConnectionFactory = new EbConnectionFactory(name, Redis);
                        EbConnectionsConfig _solutionConnections = EbConnectionsConfigProvider.GetDataCenterConnections();
                        _solutionConnections.DataDbConfig.DatabaseName = name;
                        IDatabase _ebconfactoryDatadb = new EbConnectionFactory(_solutionConnections, name).DataDB;
                        //EbConnectionsConfig _solutionConnections = EbConnectionsConfigProvider.GetDataCenterConnections();
                        //_solutionConnections.DataDbConfig.DatabaseName = "ebdbh3ivsn9lud20190808061231";
                        //IDatabase _ebconfactoryDatadb = new EbConnectionFactory(_solutionConnections, "ebdbh3ivsn9lud20190808061231").DataDB;
                        //string name = "ebdbh3ivsn9lud20190808061231";
                        string vendor = _ebconfactoryDatadb.Vendor.ToString();
                        using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                        {
                            con.Open();
                            string str = string.Format(@"
                                   SELECT * 
                                   FROM eb_dbchangeslog
                                   WHERE solution_id = '{0}'", name);
                            EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
                            if (dt != null && dt.Rows.Count > 0)
                            {
                                string str1 = string.Format(@"
                                   SELECT MAX(last_modified)  
                                   FROM eb_dbchangeslog
                                   WHERE solution_id = '{0}'", name);
                                EbDataTable dt2 = InfraConnectionFactory.DataDB.DoQuery(str1);

                                string str2 = string.Format(@"
                                            SELECT email, fullname 
                                            FROM eb_tenants
                                            WHERE id = (
                                                SELECT tenant_id 
                                                FROM eb_solutions
                                                WHERE isolution_id = '{0}')", name);
                                EbDataTable dt1 = InfraConnectionFactory.DataDB.DoQuery(str2);
                                if (dt1 != null && dt.Rows.Count > 0)
                                {
                                    list.Add(new Eb_Changes_Log
                                    {
                                        Solution = name,
                                        DBName = dt.Rows[0]["dbname"].ToString(),
                                        TenantName = dt1.Rows[0]["fullname"].ToString(),
                                        TenantEmail = dt1.Rows[0]["email"].ToString(),
                                        Last_Modified = DateTime.Parse(dt2.Rows[0][0].ToString()),
                                        Vendor = dt.Rows[0]["vendor"].ToString()
                                    });
                                }
                            }
                            else
                            {
                                string str2 = string.Format(@"
                                                SELECT * 
                                                FROM eb_solutions
                                                WHERE isolution_id = '{0}'", name);
                                EbDataTable dt2 = InfraConnectionFactory.DataDB.DoQuery(str2);

                                string str3 = string.Format(@"
                                            INSERT INTO 
                                                eb_dbchangeslog (solution_id, dbname, vendor, last_modified)
                                            VALUES ('{0}','{1}','{2}','{3}')", dt2.Rows[0]["isolution_id"].ToString(), _ebconfactoryDatadb.DBName, _ebconfactoryDatadb.Vendor, dt2.Rows[0]["eb_created_at"].ToString());
                                DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str2);
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }
                else
                {
                    int a = 0;
                }
            resp.ChangesLog = list;
            return resp;

        }

        void GetFuncScriptFromTenant(string solname)
        {
            string str = string.Empty;
            StringBuilder hash = new StringBuilder();
            MD5CryptoServiceProvider md5provider = new MD5CryptoServiceProvider();
            string result = string.Empty;
            string file_name;
            //  EbConnectionFactory factory = new EbConnectionFactory("ebdbh3ivsn9lud20190808061231", this.Redis);
            EbConnectionsConfig _solutionConnections = EbConnectionsConfigProvider.GetDataCenterConnections();
            _solutionConnections.DataDbConfig.DatabaseName = solname;
            IDatabase _ebconfactoryDatadb = new EbConnectionFactory(_solutionConnections, solname).DataDB; ;
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
                        file_name = GetFileName(result, dt.Rows[i][1].ToString());
                        result = FormatDBStringPGSQL(result);
                        byte[] bytes = md5provider.ComputeHash(new UTF8Encoding().GetBytes(result));
                        hash.Clear();
                        for (int j = 0; j < bytes.Length; j++)
                        {
                            hash.Append(bytes[j].ToString("x2"));
                        }
                        dictTenant.Add(file_name, new[] { vendor, hash.ToString() });
                    }
                }
            }

            CompareScripts(_ebconfactoryDatadb);
        }


        void GetFuncScriptFromTenant()
        {
            string str = string.Empty;
            StringBuilder hash = new StringBuilder();
            MD5CryptoServiceProvider md5provider = new MD5CryptoServiceProvider();
            string result = string.Empty;
            string file_name;
            //  EbConnectionFactory factory = new EbConnectionFactory("ebdbh3ivsn9lud20190808061231", this.Redis);
            EbConnectionsConfig _solutionConnections = EbConnectionsConfigProvider.GetDataCenterConnections();
            _solutionConnections.DataDbConfig.DatabaseName = "ebdbh3ivsn9lud20190808061231";
            IDatabase _ebconfactoryDatadb = new EbConnectionFactory(_solutionConnections, "ebdbh3ivsn9lud20190808061231").DataDB;
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
                        file_name = GetFileName(result, dt.Rows[i][1].ToString());
                        result = FormatDBStringPGSQL(result);
                        byte[] bytes = md5provider.ComputeHash(new UTF8Encoding().GetBytes(result));
                        hash.Clear();
                        for (int j = 0; j < bytes.Length; j++)
                        {
                            hash.Append(bytes[j].ToString("x2"));
                        }
                        dictTenant.Add(file_name, new[] { vendor, hash.ToString() });
                    }
                }
            }
            CompareScripts(_ebconfactoryDatadb);
        }

        void GetFuncScriptFromInfra()
        {
            //string vendor = this.InfraConnectionFactory.DataDB.Vendor.ToString();
            string vendor = "PGSQL";
            string type = "FUNCTION";
            dictInfra.Clear();
            string str = string.Format(@"
                        SELECT d.change_id, d.filename, d.md5, c.filepath
                        FROM eb_dbmd5 as d, eb_dbstructure as c
                        WHERE c.vendor = '{0}'
                        AND c.type='{1}'
                        AND d.eb_del = 'F'
                        AND c.id = d.change_id", vendor, type);
            EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
            if (dt != null && dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    dictInfra.Add(dt.Rows[i]["filename"].ToString(), new Eb_FileChanges
                    {
                        Id = dt.Rows[i]["change_id"].ToString(),
                        Vendor = vendor,
                        FilePath = dt.Rows[i]["filepath"].ToString(),
                        MD5 = dt.Rows[i]["md5"].ToString().Trim()
                    });
                }
            }
        }

        void CompareScripts(IDatabase _ebconfactoryDatadb)
        {
            List<Eb_FileChanges> ChangesList = new List<Eb_FileChanges>();
            if (true)
            {
                foreach (KeyValuePair<string, Eb_FileChanges> pair in dictInfra)
                {
                    if (dictTenant.TryGetValue(pair.Key, out string[] value))
                    {
                        if (value[1] != pair.Value.MD5)
                        {
                            Console.WriteLine(pair.Key + " : " + pair.Value.FilePath + " : " + value[1] + " : change found");
                            if (dictDBFunctionChanges.ContainsKey(_ebconfactoryDatadb.DBName))
                            {
                                ChangesList = dictDBFunctionChanges[_ebconfactoryDatadb.DBName];
                                ChangesList.Add(new Eb_FileChanges
                                {
                                    Id = pair.Value.Id,
                                    FunctionHeader = pair.Key,
                                    FilePath = pair.Value.FilePath,
                                    Vendor = pair.Value.Vendor,
                                    MD5 = pair.Value.MD5
                                });
                                dictDBFunctionChanges[_ebconfactoryDatadb.DBName] = ChangesList;
                            }
                            else
                            {
                                ChangesList.Add(new Eb_FileChanges
                                {
                                    Id = pair.Value.Id,
                                    FunctionHeader = pair.Key,
                                    FilePath = pair.Value.FilePath,
                                    Vendor = pair.Value.Vendor,
                                    MD5 = pair.Value.MD5
                                });
                                dictDBFunctionChanges.Add(_ebconfactoryDatadb.DBName, ChangesList);
                            }

                            //UpdateDB(pair.Value[0], pair.Key, pair.Value[1], _ebconfactoryDatadb);
                        }
                        if (value == null)
                        {
                            Console.WriteLine(pair.Key + " : not exists in db");
                        }

                    }
                    else
                    {
                    }
                }
            }
        }



        void SetFuncMd5InfraReference(string name, IDatabase _ebconfactoryDatadb)
        {
            StringBuilder hash = new StringBuilder();
            MD5CryptoServiceProvider md5provider = new MD5CryptoServiceProvider();
            string result = string.Empty;
            string file_name = string.Empty;
            string file_name_shrt = string.Empty;
            string type = "FUNCTION";
            string[] func_create ={"datadb.functioncreate.eb_authenticate_anonymous.sql",
                        "datadb.functioncreate.eb_authenticate_unified.sql", "datadb.functioncreate.eb_createormodifyuserandroles.sql",
                        "datadb.functioncreate.eb_createormodifyusergroup.sql",  "datadb.functioncreate.eb_create_or_update_rbac_roles.sql",
                        "datadb.functioncreate.eb_create_or_update_role.sql",  "datadb.functioncreate.eb_create_or_update_role2loc.sql",
                        "datadb.functioncreate.eb_create_or_update_role2role.sql", "datadb.functioncreate.eb_create_or_update_role2user.sql",
                        "datadb.functioncreate.eb_currval_new.sql", "datadb.functioncreate.eb_getconstraintstatus.sql", "datadb.functioncreate.eb_getpermissions.sql",
                        "datadb.functioncreate.eb_getroles.sql", "datadb.functioncreate.eb_persist_currval.sql", "datadb.functioncreate.eb_revokedbaccess2user_new.sql",
                        "objectsdb.functioncreate.eb_botdetails.sql", "objectsdb.functioncreate.eb_createbot.sql",
                        "objectsdb.functioncreate.eb_get_tagged_object.sql", "objectsdb.functioncreate.eb_objects_change_status.sql", "objectsdb.functioncreate.eb_objects_commit.sql",
                        "objectsdb.functioncreate.eb_objects_create_new_object.sql", "objectsdb.functioncreate.eb_objects_exploreobject.sql",
                        "objectsdb.functioncreate.eb_objects_getversiontoopen.sql", "objectsdb.functioncreate.eb_objects_save.sql",
                        "objectsdb.functioncreate.eb_objects_update_dashboard.sql", "objectsdb.functioncreate.eb_object_create_major_version.sql",
                        "objectsdb.functioncreate.eb_object_create_minor_version.sql", "objectsdb.functioncreate.eb_object_create_patch_version.sql",
                        "objectsdb.functioncreate.eb_update_rel.sql", "objectsdb.functioncreate.split_str_util.sql", "objectsdb.functioncreate.string_to_rows_util.sql",
                        "objectsdb.functioncreate.str_to_tbl_grp_util.sql", "objectsdb.functioncreate.str_to_tbl_util.sql"};

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
                                file_name = GetFileName(result, file);
                                file_name_shrt = file_name.Split("(")[0];
                                if (vendor == "PGSQL")
                                {
                                    result = FormatFileStringPGSQL(result);
                                }

                                byte[] bytes = md5provider.ComputeHash(new UTF8Encoding().GetBytes(result));
                                hash.Clear();
                                for (int j = 0; j < bytes.Length; j++)
                                {
                                    hash.Append(bytes[j].ToString("x2"));
                                }
                                Console.WriteLine(file_name + "Success");

                                using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                                {
                                    con.Open();
                                    string str = string.Format(@"
                                    SELECT * 
                                    FROM eb_dbmd5 as d , eb_dbstructure as c
                                    WHERE d.filename = '{0}' 
                                        AND d.eb_del = 'F' 
                                        AND d.change_id = c.id", file_name);
                                    EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
                                    if (dt != null && dt.Rows.Count > 0)
                                    {
                                        string str1 = string.Format(@"
                                            SELECT * 
                                            FROM eb_dbmd5 as d , eb_dbstructure as c
                                            WHERE d.filename = '{0}' 
                                                AND d.eb_del = 'F' 
                                                AND d.change_id = c.id
                                                AND d.md5 <> '{1}'", file_name, hash.ToString());
                                        EbDataTable dt1 = InfraConnectionFactory.DataDB.DoQuery(str1);

                                        if (dt1 != null && dt1.Rows.Count > 0)
                                        {
                                            string str2 = string.Format(@"
                                                            SELECT id 
                                                            FROM eb_dbstructure
                                                            WHERE filename = '{0}'
                                                                AND vendor = '{1}'", file_name_shrt, vendor);
                                            EbDataTable dt2 = InfraConnectionFactory.DataDB.DoQuery(str2);

                                            string str3 = string.Format(@"
                                                    UPDATE eb_dbmd5 
                                                    SET eb_del = 'T'
                                                    WHERE filename = '{0}' 
                                                        AND eb_del = 'F'
                                                        AND change_id = '{1}'", file_name, dt2.Rows[0]["id"].ToString());
                                            DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str3);
                                            cmd.ExecuteNonQuery();

                                            string str4 = string.Format(@"
                                                    INSERT INTO 
                                                        eb_dbmd5 (change_id, filename, md5, eb_del)
                                                    VALUES ('{0}','{1}','{2}','F')", dt2.Rows[0]["id"].ToString(), file_name, hash.ToString());
                                            DbCommand cmd1 = InfraConnectionFactory.DataDB.GetNewCommand(con, str4);
                                            cmd1.ExecuteNonQuery();

                                            string str5 = string.Format(@"
                                                    UPDATE eb_dbstructure 
                                                    SET modified_date = '{0}'
                                                    WHERE id = '{1}'", DateTime.Now, dt2.Rows[0]["id"].ToString());
                                            DbCommand cmd2 = InfraConnectionFactory.DataDB.GetNewCommand(con, str5);
                                            cmd2.ExecuteNonQuery();

                                            string str6 = string.Format(@"
                                                    UPDATE eb_dbchangeslog 
                                                    SET last_modified = '{0}'
                                                    WHERE change_id = '{1}'", DateTime.Now, dt2.Rows[0]["id"].ToString());
                                            DbCommand cmd3 = InfraConnectionFactory.DataDB.GetNewCommand(con, str6);
                                            cmd3.ExecuteNonQuery();
                                        }
                                    }
                                    else
                                    {
                                        string str1 = string.Format(@"
                                            INSERT INTO 
                                                eb_dbstructure (filename, filepath, vendor, type, created_at, modified_date)
                                            VALUES ('{0}','{1}','{2}','{3}','{4}','{5}')", file_name_shrt, file, vendor, type, DateTime.Now, DateTime.Now);
                                        DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str1);
                                        cmd.ExecuteNonQuery();

                                        string str2 = string.Format(@"
                                                SELECT id 
                                                FROM eb_dbstructure
                                                WHERE filename = '{0}'
                                                    AND vendor = '{1}'", file_name_shrt, vendor);
                                        EbDataTable dt2 = InfraConnectionFactory.DataDB.DoQuery(str2);

                                        string str3 = string.Format(@"
                                                INSERT INTO 
                                                    eb_dbmd5 (change_id, filename, md5, eb_del)
                                                VALUES ('{0}','{1}','{2}','F')", dt2.Rows[0]["id"].ToString(), file_name, hash.ToString());
                                        DbCommand cmd2 = InfraConnectionFactory.DataDB.GetNewCommand(con, str3);
                                        cmd2.ExecuteNonQuery();

                                        string str4 = string.Format(@"
                                                SELECT * 
                                                FROM eb_solutions
                                                WHERE isolution_id = '{0}'", name);
                                        EbDataTable dt3 = InfraConnectionFactory.DataDB.DoQuery(str4);

                                        string str5 = string.Format(@"
                                                INSERT INTO 
                                                    eb_dbchangeslog (solution_id, dbname, vendor, last_modified, change_id)
                                                VALUES ('{0}','{1}','{2}','{3}','{4}')", name, _ebconfactoryDatadb.DBName, _ebconfactoryDatadb.Vendor, dt3.Rows[0]["date_created"].ToString(), dt2.Rows[0]["id"].ToString());
                                        DbCommand cmd3 = InfraConnectionFactory.DataDB.GetNewCommand(con, str5);
                                        cmd3.ExecuteNonQuery();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }


        DateTime UpdateDB(string id, string vendor, string filename, string filepath, IDatabase _ebconfactoryDatadb)
        {
            string result = string.Empty;
            DateTime modified_date = DateTime.Now;
            string Urlstart = string.Format("ExpressBase.Common.sqlscripts.{0}.", vendor.ToLower());
            string path = Urlstart + filepath;
            var assembly = typeof(sqlscripts).Assembly;
            using (Stream stream = assembly.GetManifestResourceStream(path))
            {
                if (stream != null)
                {
                    using (StreamReader reader = new StreamReader(stream))
                        result = reader.ReadToEnd();
                    string fun = GetFuncDef(result, filename);
                    using (DbConnection con = _ebconfactoryDatadb.GetNewConnection())
                    {
                        con.Open();
                        DbCommand cmd = _ebconfactoryDatadb.GetNewCommand(con, fun);
                        cmd.ExecuteNonQuery();

                        DbCommand cmd1 = _ebconfactoryDatadb.GetNewCommand(con, result);
                        int x = cmd1.ExecuteNonQuery();

                    }
                    using (DbConnection con = InfraConnectionFactory.DataDB.GetNewConnection())
                    {
                        con.Open();
                        string str = string.Format(@"
                                                    UPDATE eb_dbstructure 
                                                    SET modified_date = '{0}'
                                                    WHERE id = '{1}'", modified_date, id);
                        DbCommand cmd2 = InfraConnectionFactory.DataDB.GetNewCommand(con, str);
                        cmd2.ExecuteNonQuery();

                        string str1 = string.Format(@"
                                                    UPDATE eb_dbchangeslog 
                                                    SET last_modified = '{0}'
                                                    WHERE change_id = '{1}'", modified_date, id);
                        DbCommand cmd3 = InfraConnectionFactory.DataDB.GetNewCommand(con, str1);
                        cmd3.ExecuteNonQuery();
                    }
                }
            }
            return modified_date;
        }


        public UpdateDBFunctionByDBResponse Post(UpdateDBFunctionByDBRequest request)
        {
            UpdateDBFunctionByDBResponse resp = new UpdateDBFunctionByDBResponse();
            EbConnectionsConfig _solutionConnections = EbConnectionsConfigProvider.GetDataCenterConnections();
            _solutionConnections.DataDbConfig.DatabaseName = request.DBName;
            IDatabase _ebconfactoryDatadb = new EbConnectionFactory(_solutionConnections, request.DBName).DataDB;
            for (int i = 0; i < request.Changes.Count; i++)
            {
                resp.ModifiedDate = UpdateDB(request.Changes[i].Id, request.Changes[i].Vendor, request.Changes[i].FunctionHeader, request.Changes[i].FilePath, _ebconfactoryDatadb);
            }
            return resp;
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

        string GetFileName(string str, string file)
        {
            string[] fname;
            int pFrom = str.IndexOf("CREATE OR REPLACE FUNCTION public.") + "CREATE OR REPLACE FUNCTION public.".Length;
            int pTo = str.LastIndexOf("RETURNS");
            string res = string.Empty;
            if ((pTo - pFrom) > -1)
            {
                res = str.Substring(pFrom, pTo - pFrom);
                res = res.Replace("\r", "").Replace("\n", "").Replace("\t", "").Replace("  ", "").Replace(" DEFAULT NULL::text", "").Replace(" DEFAULT NULL::integer", "").Replace(" DEFAULT 0", "");
            }
            else
            {
                fname = file.Split(".");
                if (fname.Length > 1)
                {
                    res = fname[2] + "()";
                }
                else
                {
                    res = file + "()";
                }
            }
            res = res.Replace(", ", ",").Trim();
            return res;
        }

        string FormatDBStringPGSQL(string str)
        {
            str = str.Replace(" ", "").Replace("\r", "").Replace("\t", "").Replace("\n", "").Trim();
            return str;
        }

        string FormatFileStringPGSQL(string str)
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
            return str;
        }
    }
}


