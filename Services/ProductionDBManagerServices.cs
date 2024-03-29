﻿using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.ProductionDBManager;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace ExpressBase.ServiceStack.Services
{
    internal class TableOrFuncPresence
    {
        internal bool IsPresent { get; set; }
        internal int ChangeID { get; set; }
    }

    internal class SolutionDetails
    {
        internal string TenantName { get; set; }
        internal string TenantEmail { get; set; }
        internal DateTime Last_Modified { get; set; }
    }

    internal class FileDetails
    {
        internal string FileName { get; set; }
        internal string FileNameShrt { get; set; }
        internal string Content { get; set; }
        internal DBManagerType Type { get; set; }
    }

    public class ProductionDBManagerServices : EbBaseService
    {
        public ProductionDBManagerServices(IEbConnectionFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_dbf, _mqp, _mqc) { }
        Dictionary<string, string[]> dictTenant = new Dictionary<string, string[]>();
        Dictionary<string, Eb_FileDetails> dictInfra = new Dictionary<string, Eb_FileDetails>();

        public IDatabase GetTenantDB(string SolutionId)
        {
            IDatabase _ebconfactoryDatadb = null;
            try
            {
                EbConnectionFactory factory = new EbConnectionFactory(SolutionId, this.Redis, true);
                if (factory != null && factory.DataDB != null)
                    _ebconfactoryDatadb = factory.DataDB;

            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return _ebconfactoryDatadb;
        }

        [Authenticate]
        public GetSolutionForIntegrityCheckResponse Post(GetSolutionForIntegrityCheckRequest request)
        {
            GetSolutionForIntegrityCheckResponse resp = new GetSolutionForIntegrityCheckResponse();
            List<Eb_Changes_Log> list = new List<Eb_Changes_Log>();
            string solution_name = string.Empty;
            string i_solution_id = string.Empty;
            try
            {
                EbDataTable dt = SelectSolutionsFromDB(); // Will contain Solution names
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    solution_name = dt.Rows[i]["esolution_id"].ToString();
                    i_solution_id = dt.Rows[i]["isolution_id"].ToString();
                    IDatabase _ebconfactoryDatadb = GetTenantDB(i_solution_id);
                    EbDataTable dt1 = CheckInfraDbForSolution(i_solution_id); //Check Solution Details from eb_changeslog table in Infra DB
                    if (dt1 != null && dt1.Rows.Count > 0)
                    {
                        try
                        {
                            var sd = GetSolutionDetailsFromInfra(i_solution_id); // Get Solution details and place to list 
                            if (sd != null)
                                list.Add(new Eb_Changes_Log
                                {
                                    Solution = solution_name,
                                    ISolutionId = i_solution_id,
                                    DBName = _ebconfactoryDatadb.DBName,
                                    TenantName = sd.TenantName,
                                    TenantEmail = sd.TenantEmail,
                                    Last_Modified = sd.Last_Modified,
                                    Vendor = _ebconfactoryDatadb.Vendor.ToString()
                                });
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
                            if (_ebconfactoryDatadb != null)
                            {
                                try
                                {
                                    SolutionDetails sd = AddSolutionDetailsToInfraDb(i_solution_id, _ebconfactoryDatadb);
                                    list.Add(new Eb_Changes_Log
                                    {
                                        Solution = solution_name,
                                        ISolutionId = i_solution_id,
                                        DBName = _ebconfactoryDatadb.DBName,
                                        TenantName = sd.TenantName,
                                        TenantEmail = sd.TenantEmail,
                                        Last_Modified = sd.Last_Modified,
                                        Vendor = _ebconfactoryDatadb.Vendor.ToString()
                                    });
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
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                Console.WriteLine(e.Message);
            }
            resp.ChangesLog = list;
            return resp;
        }

        private EbDataTable SelectSolutionsFromDB()
        {
            string str = @"
                                SELECT * 
                                FROM eb_solutions 
                                WHERE eb_del = false 
                                AND tenant_id IN (select id from eb_tenants)
                                AND isolution_id != ''";
            return InfraConnectionFactory.DataDB.DoQuery(str);

        }

        private EbDataTable CheckInfraDbForSolution(string solution_name)
        {
            string str = string.Format(@"
                                   SELECT * 
                                   FROM infra_dbchangeslog
                                   WHERE solution_id = '{0}' ", solution_name);
            return InfraConnectionFactory.DataDB.DoQuery(str);
        }

        private SolutionDetails GetSolutionDetailsFromInfra(string solution_name)
        {
            string str = string.Format(@"
                    SELECT 
                        D.modified_at, T.email , T.fullname 
                    FROM 
                        infra_dbchangeslog AS D, eb_tenants AS T, eb_solutions AS S 
                    WHERE 
                        T.id = S.tenant_id AND D.solution_id = '{0}' AND S.isolution_id = D.solution_id  AND COALESCE(S.eb_del, FALSE) = FALSE", solution_name);

            var dt2 = InfraConnectionFactory.DataDB.DoQuery(str);

            SolutionDetails sd = null;
            if (dt2 != null && dt2.Rows.Count > 0)
            {
                sd = GetSolutionDetails(dt2);
            }
            return sd;
        }

        private SolutionDetails AddSolutionDetailsToInfraDb(string solution_name, IDatabase _ebconfactoryDatadb)
        {
            EbDataTable dt = null;
            using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                string str = string.Format(@"
                                                SELECT t.email, t.fullname, s.date_created 
                                                FROM eb_solutions as s, eb_tenants as t 
                                                WHERE s.isolution_id = '{0}'
                                                    AND s.eb_del = false
                                                    AND s.tenant_id = t.id", solution_name);

                dt = InfraConnectionFactory.DataDB.DoQuery(str);
                if (dt.Rows.Count > 0)
                {
                    string str1 = @"
                                                  INSERT INTO 
                                                      infra_dbchangeslog (solution_id, vendor, modified_at)
                                                  VALUES (:solution_name, :vendor, :modified_at)";

                    DbParameter[] parameters = { this.InfraConnectionFactory.DataDB.GetNewParameter("solution_name",Common.Structures.EbDbTypes.String, solution_name.ToString()),
                                                 this.InfraConnectionFactory.DataDB.GetNewParameter("vendor", Common.Structures.EbDbTypes.String,  _ebconfactoryDatadb.Vendor.ToString()),
                                                 this.InfraConnectionFactory.DataDB.GetNewParameter("modified_at", Common.Structures.EbDbTypes.DateTime, Convert.ToDateTime( dt.Rows[0]["date_created"]))};
                    int val = InfraConnectionFactory.DataDB.DoNonQuery(str1, parameters);
                }
            }

            SolutionDetails sd = null;
            if (dt != null && dt.Rows.Count > 0)
            {
                sd = GetSolutionDetails(dt);
            }
            return sd;
        }

        private SolutionDetails GetSolutionDetails(EbDataTable dt)
        {
            SolutionDetails sd = new SolutionDetails
            {
                TenantName = dt.Rows[0]["fullname"].ToString(),
                TenantEmail = dt.Rows[0]["email"].ToString(),
                Last_Modified = DateTime.Parse(dt.Rows[0][0].ToString())
            };
            return sd;
        }

        //Update Infra with SQL Script buttom click
        [Authenticate]
        public UpdateInfraWithSqlScriptsResponse Post(UpdateInfraWithSqlScriptsRequest request)
        {
            UpdateInfraWithSqlScriptsResponse resp = new UpdateInfraWithSqlScriptsResponse();
            resp.ErrorMessage = string.Empty;
            try
            {
                SetFileMd5InfraReference();
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                resp.ErrorMessage = e.Message;
            }
            return resp;
        }

        //Check Integrity Buttom click
        [Authenticate]
        public CheckChangesInFilesResponse Post(CheckChangesInFilesRequest request)
        {
            CheckChangesInFilesResponse resp = new CheckChangesInFilesResponse();
            List<Eb_FileDetails> ChangesList = new List<Eb_FileDetails>();
            try
            {
                IDatabase _ebconfactoryDatadb = GetTenantDB(request.SolutionId);
                if (_ebconfactoryDatadb != null)
                {
                    GetFileScriptFromInfra(_ebconfactoryDatadb);
                    ChangesList = GetFileScriptFromTenant(_ebconfactoryDatadb);
                    resp.Changes = ChangesList;
                }
                if (ChangesList == null)
                    Console.WriteLine("ChangesList is null");
                else
                    Console.WriteLine("ChangesList count: " + ChangesList.Count);
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                resp.ErrorMessage = e.Message;
                return resp;
            }
            return resp;
        }

        // Change button click
        [Authenticate]
        public UpdateDBFilesByDBResponse Post(UpdateDBFileByDBRequest request)
        {
            UpdateDBFilesByDBResponse resp = new UpdateDBFilesByDBResponse();
            CheckChangesInFilesResponse resp1 = new CheckChangesInFilesResponse();
            CheckChangesInFilesRequest req = new CheckChangesInFilesRequest();
            List<Eb_FileDetails> ChangesList = new List<Eb_FileDetails>();
            IDatabase _ebconfactoryDatadb = GetTenantDB(request.SolutionId);
            if (_ebconfactoryDatadb != null)
            {
                GetFileScriptFromInfra(_ebconfactoryDatadb);
                ChangesList = GetFileScriptFromTenant(_ebconfactoryDatadb);
                resp.ModifiedDate = UpdateDBFileByDB(ChangesList, request.SolutionId);
                req.SolutionId = request.SolutionId;
                resp1 = this.Post(req);
                ChangesList = resp1.Changes;
                resp.Changes = ChangesList;
            }

            return resp;
        }

        [Authenticate]
        public GetFunctionOrProcedureQueriesResponse Post(GetFunctionOrProcedureQueriesRequest request)
        {
            GetFunctionOrProcedureQueriesResponse resp = new GetFunctionOrProcedureQueriesResponse();
            try
            {
                IDatabase _ebconfactoryDatadb = GetTenantDB(request.SolutionId);
                resp.Query = GetFunctionOrProcedureQuery(request.ChangeList, _ebconfactoryDatadb);
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                resp.ErrorMessage = e.Message;
            }
            return resp;
        }

        [Authenticate]
        public GetTableQueriesResponse Post(GetTableQueriesRequest request)
        {
            GetTableQueriesResponse resp = new GetTableQueriesResponse();
            try
            {
                IDatabase _ebconfactoryDatadb = GetTenantDB(request.SolutionId);
                resp.Query = UpdateTableOnTenant(request.ChangeList, _ebconfactoryDatadb, false, request.SolutionId);
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                resp.ErrorMessage = e.Message;
            }
            return resp;
        }

        [Authenticate]
        public ExecuteQueriesResponse Post(ExecuteQueriesRequest request)
        {
            ExecuteQueriesResponse resp = new ExecuteQueriesResponse();
            List<Eb_FileDetails> ChangesList = new List<Eb_FileDetails>();
            try
            {
                IDatabase _ebconfactoryDatadb = GetTenantDB(request.SolutionId);
                resp.ErrorMessage = ExecuteQuery(request.Query, _ebconfactoryDatadb, request.SolutionId);
                GetAFileScriptFromInfra(_ebconfactoryDatadb, request.FileName);
                ChangesList = GetAFileScriptFromTenant(_ebconfactoryDatadb, request.FileName, request.FileType);
                if (ChangesList.Count != 0 && resp.ErrorMessage == "")
                {
                    resp.Changes = ChangesList;
                    resp.ErrorMessage = "Execution Completed !!! Changes Found!!!";
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                resp.Changes = null;
                resp.ErrorMessage = e.Message;
            }

            return resp;
        }

        [Authenticate]
        public GetScriptsForDiffViewResponse Post(GetScriptsForDiffViewRequest request)
        {
            GetScriptsForDiffViewResponse resp = new GetScriptsForDiffViewResponse();
            try
            {
                IDatabase _ebconfactoryDatadb = GetTenantDB(request.SolutionId);
                List<string> contents = GetScriptContentForDiffView(request.FileHeader, request.FilePath, request.FileType, _ebconfactoryDatadb);
                resp.InfraFileContent = contents[0];
                resp.TenantFileContent = contents[1];
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                resp.ErrorMessage = e.Message;
            }
            return resp;
        }

        //----------------------------------------------------------------------------------------------------------------------------

        //-------------------------------------------Insert or Update MD5 or JSON to Infra--------------------------------------------

        void SetFileMd5InfraReference()
        {
            string md5_or_json = string.Empty;
            string result = string.Empty;
            string file_name = string.Empty;
            string file_name_shrt = string.Empty;
            string[] func_create = SqlFiles.SQLSCRIPTS;
            try
            {
                RemoveUnWantedFilesFromInfraTables();
                foreach (string vendor in Enum.GetNames(typeof(DatabaseVendors)))
                {
                    if (vendor == "PGSQL" || vendor == "MYSQL")
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
                                        FileDetails fd = GetFileDetailsFromScript(result, vendor, file);
                                        InsertOrUpdateValuesInInfraDB(fd.FileName, vendor, fd.Content, fd.FileNameShrt, file, fd.Type);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
        }

        private void RemoveUnWantedFilesFromInfraTables()
        {
            List<string> files_from_sqlscript = GetFilesListFromSqlScripts();
            List<string> files_from_infra_table = GetFilesListFromInfraTable();
            if (files_from_infra_table.Count > 0)
            {
                List<string> unwanted_files = UnwantedFiles(files_from_infra_table, files_from_sqlscript);
                using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                {
                    con.Open();
                    string str = string.Empty;
                    foreach (string file_name in unwanted_files)
                    {
                        int change_id = 0;
                        string str1 = string.Format(@"select change_id from infra_dbmd5 where filename = '{0}' and eb_del = 'F';",
                                            file_name);
                        EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str1);
                        if (dt.Rows.Count > 0)
                        {
                            change_id = (int)dt.Rows[0][0];
                            str += string.Format(@"update infra_dbmd5 set eb_del = 'T' where change_id = {0};",
                                            change_id);
                            str += string.Format(@"update infra_dbstructure set eb_del = 'T' where id = {0};", change_id);
                        }
                    }
                    using (DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        private List<string> GetFilesListFromSqlScripts()
        {
            string[] func_create = SqlFiles.SQLSCRIPTS;
            string result = string.Empty;
            DBManagerType type = DBManagerType.Function;
            List<string> file_name = new List<string>();
            foreach (string vendor in Enum.GetNames(typeof(DatabaseVendors)))
            {
                if (vendor == "PGSQL" || vendor == "MYSQL")
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
                                if (file.Split(".")[1] == "functioncreate")
                                {
                                    type = CheckFunctionOrProcedure(result);
                                    file_name.Add(GetFileName(result, file, type, vendor)); //function name with parameter
                                }
                                else if (file.Split(".")[1] == "tablecreate")
                                {
                                    type = DBManagerType.Table;
                                    file_name.Add(GetFileName(result, file, type, vendor)); //table name
                                }
                            }
                        }
                    }
                }
            }
            return file_name;
        }

        private List<string> GetFilesListFromInfraTable()
        {
            List<string> file_name = new List<string>();
            string str = @"
                           SELECT DISTINCT filename 
                           FROM infra_dbmd5";
            EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    file_name.Add(dt.Rows[i][0].ToString());
                }
            }
            return file_name;
        }

        private List<string> UnwantedFiles(List<string> files_from_infra_table, List<string> files_from_sqlscript)
        {
            List<string> unwanted_files = new List<string>();
            int f = 0;
            foreach (string file_from_infra_table in files_from_infra_table)
            {
                foreach (string file_from_script in files_from_sqlscript)
                {
                    if (file_from_infra_table == file_from_script)
                        f = 1;
                }
                if (f == 0)
                    unwanted_files.Add(file_from_infra_table);
                f = 0;
            }
            return unwanted_files;
        }

        private FileDetails GetFileDetailsFromScript(string result, string vendor, string file)
        {
            DBManagerType type = DBManagerType.Function;
            string file_name = string.Empty;
            string file_name_shrt = string.Empty;
            string md5_or_json = string.Empty;
            FileDetails fd = null;
            try
            {
                if (file.Split(".")[1] == "functioncreate")
                {
                    type = CheckFunctionOrProcedure(result);
                    file_name = GetFileName(result, file, type, vendor); //function name with parameter
                    if (file_name.Contains("("))
                        file_name_shrt = file_name.Split("(")[0]; // function name without parameter
                    else
                        file_name_shrt = file_name;
                    md5_or_json = SetFunctionOrProcedureScriptFromScripts(result, vendor, type, file_name);
                }
                else if (file.Split(".")[1] == "tablecreate")
                {
                    type = DBManagerType.Table;
                    file_name = GetFileName(result, file, type, vendor); //table name
                    file_name_shrt = file_name;
                    md5_or_json = SetTableScriptFromInfra(file_name, vendor).Replace("'", "''");
                }
                fd = new FileDetails
                {
                    FileName = file_name,
                    FileNameShrt = file_name_shrt,
                    Type = type,
                    Content = md5_or_json
                };
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return fd;
        }

        private void InsertOrUpdateValuesInInfraDB(string file_name, string vendor, string md5_or_json, string file_name_shrt, string file, DBManagerType type)
        {
            try
            {
                var t = IsTableOrFuncPresentInInfraDb(file_name, vendor);
                if (t.IsPresent)
                {
                    if (IsContentSameCheckInInfraDB(md5_or_json, t.ChangeID))
                        UpdateValuesInInfraDB(file_name, md5_or_json, t.ChangeID);
                }
                else
                    InsertNewValueToInfraDB(file_name, md5_or_json, vendor, file_name_shrt, file, type);
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
        }

        private TableOrFuncPresence IsTableOrFuncPresentInInfraDb(string file_name, string vendor)
        {
            string str = string.Format(@"
                                                SELECT * 
                                                FROM infra_dbmd5 as d , infra_dbstructure as c
                                                WHERE d.filename = '{0}' 
                                                    AND d.eb_del = 'F' 
                                                    AND c.vendor = '{1}'
                                                    AND d.change_id = c.id", file_name, vendor);
            var dt = InfraConnectionFactory.DataDB.DoQuery(str);

            return new TableOrFuncPresence { IsPresent = (dt != null && dt.Rows.Count > 0), ChangeID = (dt != null && dt.Rows.Count > 0) ? Convert.ToInt32(dt.Rows[0]["change_id"]) : 0 };
        }

        private bool IsContentSameCheckInInfraDB(string md5_or_json, int change_id)
        {
            string str = string.Format(@"
                                            SELECT * 
                                            FROM infra_dbmd5 as d , infra_dbstructure as c
                                            WHERE  d.change_id = {0}
                                                AND c.id = {0}
                                                AND d.eb_del = 'F'
                                                AND d.contents <> '{1}'", change_id, md5_or_json);
            var dt = InfraConnectionFactory.DataDB.DoQuery(str);
            return (dt != null && dt.Rows.Count > 0);
        }

        private void UpdateValuesInInfraDB(string file_name, string md5_or_json, int change_id)
        {
            try
            {
                using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                {
                    con.Open();
                    string str = string.Format(@"UPDATE infra_dbmd5 SET eb_del = 'T' WHERE filename = '{0}' AND eb_del = 'F' AND change_id = '{1}';",
                        file_name, change_id);
                    str += string.Format(@"INSERT INTO infra_dbmd5 (change_id, filename, contents, eb_del) VALUES ('{0}','{1}','{2}','F')",
                        change_id, file_name, md5_or_json);

                    using (DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
        }

        private void InsertNewValueToInfraDB(string file_name, string content, string vendor, string file_name_shrt, string file, DBManagerType type)
        {
            try
            {
                using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                {
                    con.Open();
                    string str = string.Format(@"INSERT INTO infra_dbstructure (filename, filepath, vendor, type, eb_del) VALUES ('{0}','{1}','{2}','{3}','F');",
                        file_name_shrt, file, vendor, (int)type);
                    str += string.Format(@"INSERT INTO  infra_dbmd5 (change_id, filename, contents, eb_del) VALUES ((SELECT max(id) FROM infra_dbstructure WHERE filename = '{0}' AND vendor = '{1}' AND eb_del = 'F'),'{2}','{3}','F')",
                        file_name_shrt, vendor, file_name, content);

                    using (DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace + "\n FileName : " + file_name_shrt);
                throw e;
            }
        }

        //---------------------------------------------------SET MD5 OR JSON ON INFRA---------------------------------------------------

        string SetFunctionOrProcedureScriptFromScripts(string result, string vendor, DBManagerType type, string filename)
        {
            try
            {
                if (vendor == "PGSQL")
                    result = FormatFileStringPGSQL(result, DBManagerType.Function, filename); //remove escape sequences, spaces
                else if (vendor == "MYSQL")
                {
                    if (type == DBManagerType.Function)
                        result = FormatFileStringMYSQL(result, DBManagerType.Function, filename); //remove escape sequences, spaces
                    else if (type == DBManagerType.Procedure)
                        result = FormatFileStringMYSQL(result, DBManagerType.Procedure, filename); //remove escape sequences, spaces
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return StringToMD5Converter(result);
        }

        private string StringToMD5Converter(string result)
        {
            Console.WriteLine("Tenant Func StringToMD5Converter... Line No: 684");
            StringBuilder hash = new StringBuilder();
            MD5CryptoServiceProvider md5provider = new MD5CryptoServiceProvider();
            byte[] bytes = md5provider.ComputeHash(new UTF8Encoding().GetBytes(result));
            hash.Clear();
            for (int j = 0; j < bytes.Length; j++)
            {
                hash.Append(bytes[j].ToString("x2"));
            }
            return hash.ToString();
        }

        string SetTableScriptFromInfra(string table_name, string vendor)
        {
            string json = string.Empty;
            string str = String.Format(@"
                                            SELECT 
	                                            C.column_name, C.data_type, C.column_default, TC.constraint_type, TC.constraint_name 
                                            FROM 
	                                            information_schema.columns C
                                            LEFT JOIN 
	                                            information_schema.key_column_usage KC 
                                            ON 
	                                            (C.table_schema = KC.table_schema and C.table_name = KC.table_name  AND C.column_name = KC.column_name)
                                            LEFT JOIN 
	                                            information_schema.table_constraints TC
                                            ON
	                                            (KC.table_name = TC.table_name  AND KC.constraint_name = TC.constraint_name)	
                                            WHERE 
	                                            C.table_name = '{0}'
                                            ORDER BY  C.column_name;

                                            SELECT indexname, indexdef 
		                                    FROM 
			                                    pg_indexes
		                                    WHERE tablename = '{0}'
                                            AND indexname NOT LIKE '%_pkey'
                                            ORDER BY indexname
                                    ", table_name);
            DbParameter[] parameters = { this.InfraConnectionFactory.DataDB.GetNewParameter("table_name", Common.Structures.EbDbTypes.String, table_name) };
            EbDataSet dt = InfraConnectionFactory.DataDB.DoQueries(str, parameters);
            json = CreateTableJsonForInfra(table_name, dt, vendor).Replace("'", "''");
            return json;
        }

        //---------------------------------------------------------Table Json---------------------------------------------------------

        string CreateTableJsonForInfra(string table_name, EbDataSet dt, string vendor)
        {
            string json = string.Empty;
            Eb_TableFieldChangesList changes = new Eb_TableFieldChangesList();
            changes.Table_name = table_name;
            try
            {
                if (dt != null && dt.Tables.Count > 0)
                {
                    changes.Fields = InfraFieldsDetailsForJsonConvert(table_name, dt.Tables[0], vendor);
                    changes.Indexs = GetInfraIndexDetailsForJSON(dt.Tables[1], vendor);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
            }
            json = JsonConvert.SerializeObject(changes);
            return json;
        }

        Dictionary<string, List<Eb_TableFieldChanges>> InfraFieldsDetailsForJsonConvert(string table_name, EbDataTable dt, string vendor)
        {
            DbTypeMap4IntegrityCollection DbTypeIntegrityCollection = DBIntegrityCollection.DataTypeCollection;
            DbColumnDefaultMap4IntegrityCollection DbColumnDefaultIntegrityCollection = DBIntegrityCollection.ColumnDefaultCollection;
            MySQLKeywordsConvertCollection MySQLKeywordsConvertCollection = DBIntegrityCollection.ColumnName;
            Dictionary<string, List<Eb_TableFieldChanges>> Fields = new Dictionary<string, List<Eb_TableFieldChanges>>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string column_default = dt.Rows[i]["column_default"].ToString();
                string constraint_name = dt.Rows[i]["constraint_name"].ToString();
                string data_type = dt.Rows[i]["data_type"].ToString();
                string column_name = dt.Rows[i]["column_name"].ToString();
                string constraint_type = dt.Rows[i]["constraint_type"].ToString();
                if (vendor == "MYSQL")
                {
                    column_default = CheckForSequences(column_default);
                    constraint_name = CheckForPrimaryConstraint(constraint_name);
                    if (MySQLKeywordsConvertCollection.ContainsKey(column_name))
                        column_name = MySQLKeywordsConvertCollection.GetColumnName(column_name);
                    if (data_type == "text")
                    {
                        if (column_default != "")
                            data_type = "varchar";
                        if (constraint_type == "UNIQUE")
                            data_type = "varchar";
                        data_type = IndexCheckForMysqlDatatypeInPostgres(data_type, column_name, table_name);
                    }
                }
                if (vendor != "PGSQL")
                    column_default = DbColumnDefaultIntegrityCollection.GetColumnDefault((DatabaseVendors)Enum.Parse(typeof(DatabaseVendors), vendor), column_default);
                Fields.Add(column_name, new List<Eb_TableFieldChanges> {
                        new Eb_TableFieldChanges{
                            Data_type = DbTypeIntegrityCollection.GetDbType((DatabaseVendors)Enum.Parse(typeof(DatabaseVendors), vendor),data_type),
                            Column_default = column_default,
                            Constraint_name = constraint_name,
                            Constraint_type = constraint_type
                        }
                    });
            }
            return Fields;
        }

        Dictionary<string, string> GetInfraIndexDetailsForJSON(EbDataTable dt, string vendor)
        {
            Dictionary<string, string> Indexs = new Dictionary<string, string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string indexdef = dt.Rows[i]["indexdef"].ToString();
                if (indexdef.Contains("USING btree") && vendor == "MYSQL")
                    indexdef = indexdef.Replace("USING btree", "").Replace(")", ") using btree");
                if (indexdef.Contains("public.") && vendor == "MYSQL")
                    indexdef = indexdef.Replace("public.", "");
                Indexs.Add(
                    dt.Rows[i]["indexname"].ToString(),
                    indexdef.Replace("  ", " ")
                    );
            }
            return Indexs;
        }

        string CheckForSequences(string column_default)
        {
            if (column_default.Contains("nextval")) // If Sequence
            {
                Regex regex = new Regex(@"\(.*?\)");
                MatchCollection matches = regex.Matches(column_default);
                column_default = column_default.Replace(matches[0].ToString(), "");
            }
            return column_default;
        }

        string CheckForPrimaryConstraint(string constraint_name)
        {
            if (constraint_name.Contains("pkey"))
                constraint_name = "PRIMARY";
            return constraint_name;
        }

        string GetTableScriptFromTenant(string table_name, IDatabase _ebconfactoryDatadb, string vendor)
        {
            EbDataSet dt = null;
            string json = string.Empty;
            string str = string.Empty;
            Console.WriteLine("Tenant Table script... Line No: 835");
            try
            {
                if (vendor == "PGSQL")
                    str = @"
                                            SELECT 
	                                            DISTINCT C.column_name as column_name, C.data_type as data_type, C.column_default as column_default, TC.constraint_type as constraint_type, TC.constraint_name as constraint_name
                                            FROM 
	                                            information_schema.columns C
                                            LEFT JOIN 
	                                            information_schema.key_column_usage KC 
                                            ON 
	                                            (C.table_schema = KC.table_schema and C.table_name = KC.table_name  AND C.column_name = KC.column_name)
                                            LEFT JOIN 
	                                            information_schema.table_constraints TC
                                            ON
	                                            (KC.table_name = TC.table_name  AND KC.constraint_name = TC.constraint_name)	
                                            WHERE 
	                                            C.table_name = :table_name
                                            ORDER BY  C.column_name;
                                            SELECT indexname, indexdef 
		                                    FROM 
			                                    pg_indexes
		                                    WHERE tablename = :table_name
                                            AND indexname NOT LIKE '%_pkey'
                                            ORDER BY indexname;
                                    ";
                else if (vendor == "MYSQL")
                    str += @"
                                            SELECT 
	                                            DISTINCT C.column_name as column_name, C.data_type as data_type, C.column_default as column_default, TC.constraint_type as constraint_type, TC.constraint_name as constraint_name
                                            FROM 
	                                            information_schema.columns C
                                            LEFT JOIN 
	                                            information_schema.key_column_usage KC 
                                            ON 
	                                            ( C.table_schema = KC.table_schema and C.table_name = KC.table_name  AND C.column_name = KC.column_name )
                                            LEFT JOIN 
	                                            information_schema.table_constraints TC
                                            ON
	                                            (KC.table_name = TC.table_name  AND KC.constraint_name = TC.constraint_name)	
                                            WHERE 
	                                            C.table_name = :table_name AND C.table_schema = :db_name
                                            ORDER BY  C.column_name;

                                        SELECT a.index_name as indexname, concat('CREATE',a.is_unique ,'INDEX ', a.index_name, ' ON ', a.table_name,' (',a.index_columns,')', ' USING ', a.index_type) as indexdef
                                        FROM (
                                        SELECT DISTINCT index_name,
                                               group_concat(column_name order by seq_in_index) as index_columns,
                                               index_type,
                                               case non_unique
                                                    when 1 then ' '
                                                    else ' UNIQUE '
                                                    end as is_unique,
                                                table_name
                                        FROM information_schema.statistics
                                        WHERE table_name = :table_name AND index_name <> 'PRIMARY' AND table_schema = :db_name
                                        GROUP BY index_schema,
                                                 index_name,
                                                 index_type,
                                                 non_unique,
                                                 table_name) a;
                                        ";

                DbParameter[] parameters = { _ebconfactoryDatadb.GetNewParameter("table_name", Common.Structures.EbDbTypes.String, table_name),
                                            _ebconfactoryDatadb.GetNewParameter("db_name", Common.Structures.EbDbTypes.String, _ebconfactoryDatadb.DBName.ToString())};
                dt = _ebconfactoryDatadb.DoQueries(str, parameters);

                json = CreateTableJson(table_name, dt, vendor);
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return json;
        }

        string CreateTableJson(string table_name, EbDataSet dt, string vendor)
        {
            string json = string.Empty;
            Eb_TableFieldChangesList changes = new Eb_TableFieldChangesList();
            Dictionary<string, string> Indexs = new Dictionary<string, string>();
            Console.WriteLine("Tenant Table Json... Line No: 918");
            changes.Table_name = table_name;
            try
            {
                if (dt != null && dt.Tables.Count > 0)
                {
                    changes.Fields = TenantFieldsDetailsForJsonConvert(dt.Tables[0], vendor);
                    changes.Indexs = GetTenantIndexDetailsForJSON(dt.Tables[1], table_name);
                }
                json = JsonConvert.SerializeObject(changes);
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return json;
        }

        Dictionary<string, List<Eb_TableFieldChanges>> TenantFieldsDetailsForJsonConvert(EbDataTable dt, string vendor)
        {
            Dictionary<string, List<Eb_TableFieldChanges>> Fields = new Dictionary<string, List<Eb_TableFieldChanges>>();
            MySQLKeywordsConvertCollection MySQLKeywordsConvertCollection = DBIntegrityCollection.ColumnName;
            DbColumnDefaultMap4IntegrityCollection DbColumnDefaultIntegrityCollection = DBIntegrityCollection.ColumnDefaultCollection;
            try
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    if (!Fields.ContainsKey(dt.Rows[i]["column_name"].ToString()))
                    {
                        string column_default = dt.Rows[i]["column_default"].ToString();
                        string constraint_name = dt.Rows[i]["constraint_name"].ToString();
                        string data_type = dt.Rows[i]["data_type"].ToString();
                        string column_name = dt.Rows[i]["column_name"].ToString();
                        string constraint_type = dt.Rows[i]["constraint_type"].ToString();
                        if (vendor == "MYSQL")
                        {
                            if (MySQLKeywordsConvertCollection.ContainsKey(column_name))
                                column_name = MySQLKeywordsConvertCollection.GetColumnName(column_name);
                        }
                        Fields.Add(column_name, new List<Eb_TableFieldChanges> {
                            new Eb_TableFieldChanges{
                                Data_type = data_type,
                                Column_default = column_default,
                                Constraint_name = constraint_name,
                                Constraint_type = constraint_type
                            }
                        });
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return Fields;
        }

        Dictionary<string, string> GetTenantIndexDetailsForJSON(EbDataTable dt, string table_name)
        {
            Dictionary<string, string> Indexs = new Dictionary<string, string>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string indexname = dt.Rows[i]["indexname"].ToString();
                string indexdef = dt.Rows[i]["indexdef"].ToString();
                if (!indexdef.Contains("public."))
                {
                    indexdef = indexdef.Replace(table_name + " ", "public." + table_name + " ");
                }

                if (!Indexs.ContainsKey(dt.Rows[i]["indexname"].ToString()))
                {
                    Indexs.Add(
                    indexname,
                    indexdef.Replace("USING BTREE", "using btree").Replace("  ", " ")
                    );
                }
            }
            return Indexs;
        }

        //-------------------------------------------------GET MD5 or JSON FROM INFRA--------------------------------------------------- 

        void GetFileScriptFromInfra(IDatabase _ebconfactoryDatadb)
        {
            try
            {
                string vendor = _ebconfactoryDatadb.Vendor.ToString();
                dictInfra.Clear();
                string str = string.Format(@"
                        SELECT d.change_id, d.filename, d.contents, c.filepath, c.type
                        FROM infra_dbmd5 as d, infra_dbstructure as c
                        WHERE c.vendor = '{0}'
                        AND d.eb_del = 'F'
                        AND c.eb_del = 'F'
                        AND c.id = d.change_id
                        AND (c.type = '0'
                        OR c.type='1'
                        OR c.type ='2')
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
                            Content = dt.Rows[i]["contents"].ToString().Contains("''") ? dt.Rows[i]["contents"].ToString().Replace("''", "'").Trim() : dt.Rows[i]["contents"].ToString().Trim(),
                            Type = (DBManagerType)Convert.ToInt32(dt.Rows[i]["type"])
                        });
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }

        }

        void GetAFileScriptFromInfra(IDatabase _ebconfactoryDatadb, string filename)
        {
            try
            {
                string vendor = _ebconfactoryDatadb.Vendor.ToString();
                dictInfra.Clear();
                string str = string.Format(@"
                        SELECT d.change_id, d.filename, d.contents, c.filepath, c.type
                        FROM infra_dbmd5 as d, infra_dbstructure as c
                        WHERE c.vendor = '{0}'
                        AND d.eb_del = 'F'
                        AND c.id = d.change_id
                        AND d.filename = '{1}'", vendor, filename);
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
                            Content = dt.Rows[i]["contents"].ToString().Contains("''") ? dt.Rows[i]["contents"].ToString().Replace("''", "'").Trim() : dt.Rows[i]["contents"].ToString().Trim(),
                            Type = (DBManagerType)Convert.ToInt32(dt.Rows[i]["type"])
                        });
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }

        }

        //-------------------------------------------------GET MD5 or JSON FROM TENANT -------------------------------------------------

        List<Eb_FileDetails> GetFileScriptFromTenant(IDatabase _ebconfactoryDatadb)
        {
            try
            {
                Console.WriteLine("Tenant File Details... Line No: 1086");
                dictTenant.Clear();
                GetFunctionDetailsFromTenant(_ebconfactoryDatadb);
                GetTableDetailsFromTenant(_ebconfactoryDatadb);
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return CompareScripts(_ebconfactoryDatadb);
        }

        List<Eb_FileDetails> GetAFileScriptFromTenant(IDatabase _ebconfactoryDatadb, string filename, string filetype)
        {
            try
            {
                dictTenant.Clear();
                if (filetype == "Table")
                    GetATableDetailsFromTenant(_ebconfactoryDatadb, filename);
                else
                    GetAFunctionDetailsFromTenant(_ebconfactoryDatadb, filename);

            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return CompareScripts(_ebconfactoryDatadb);
        }

        void GetFunctionDetailsFromTenant(IDatabase _ebconfactoryDatadb)
        {
            try
            {
                Console.WriteLine("Tenant Func Details... Line No: 1122");
                string MD5 = string.Empty;
                string result = string.Empty;
                string str = string.Empty;
                string file_name = string.Empty;
                DBManagerType type = 0;
                string vendor = _ebconfactoryDatadb.Vendor.ToString();
                if (vendor == "PGSQL")
                {
                    str = @"
                            SELECT pg_get_functiondef(oid)::text as definition, proname as routine_name
                            FROM pg_proc 
                            WHERE proname 
                            IN 
                                (SELECT routine_name 
                                FROM information_schema.routines 
                                WHERE routine_type = 'FUNCTION' 
                                    AND specific_schema = 'public'
                                    AND routine_name like 'eb_%')";
                }
                else if (vendor == "MYSQL")
                {
                    str = String.Format(@"
                                        SELECT routine_schema AS database_name,
                                               routine_name AS routine_name,
                                               CASE routine_type
                                               WHEN 'FUNCTION' THEN 0
                                               ELSE 2
                                               END
                                               AS type,
                                               data_type AS return_type,
                                               routine_definition AS definition
                                        FROM information_schema.routines
                                        WHERE routine_schema NOT IN ('sys', 'information_schema',
                                                                     'mysql', 'performance_schema')
                                        AND routine_schema = '{0}'
                                        AND routine_name like 'eb_%'
                                        ORDER BY routine_schema,
                                                 routine_name;
                            ", _ebconfactoryDatadb.DBName);
                }
                EbDataTable dt = _ebconfactoryDatadb.DoQuery(str);
                if (dt != null && dt.Rows.Count > 0)
                {
                    for (int i = 0; i < dt.Rows.Count; i++)
                    {
                        result = dt.Rows[i]["definition"].ToString();
                        if (vendor == "PGSQL")
                        {
                            type = DBManagerType.Function;
                            file_name = GetFileName(result, dt.Rows[i]["routine_name"].ToString(), DBManagerType.Function, vendor); // function name with parameters
                            result = FormatDBStringPGSQL(result, DBManagerType.Function);
                        }
                        else if (vendor == "MYSQL")
                        {
                            type = (DBManagerType)dt.Rows[i]["type"];
                            file_name = dt.Rows[i]["routine_name"].ToString();
                            result = result.Replace(" ", "").Replace("\r", "").Replace("\t", "").Replace("\n", "");
                        }

                        MD5 = StringToMD5Converter(result);
                        dictTenant.Add(file_name, new[] { vendor, MD5, type.ToString() });
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
        }

        void GetAFunctionDetailsFromTenant(IDatabase _ebconfactoryDatadb, string filename)
        {
            try
            {
                string MD5 = string.Empty;
                string result = string.Empty;
                string str = string.Empty;
                string file_name = string.Empty;
                string file_name_shrt = filename.Split("(")[0];
                DBManagerType type = 0;
                string vendor = _ebconfactoryDatadb.Vendor.ToString();
                if (vendor == "PGSQL")
                {
                    str = String.Format(@"
                            SELECT pg_get_functiondef(oid)::text as definition, proname as routine_name
                            FROM pg_proc 
                            WHERE proname 
                            IN 
                                (SELECT routine_name 
                                FROM information_schema.routines 
                                WHERE routine_type = 'FUNCTION' 
                                    AND specific_schema = 'public'
                                    AND routine_name = '{0}')", file_name_shrt);
                }
                else if (vendor == "MYSQL")
                {
                    str = String.Format(@"
                                        SELECT routine_schema AS database_name,
                                               routine_name AS routine_name,
                                               CASE routine_type
                                               WHEN 'FUNCTION' THEN 0
                                               ELSE 2
                                               END
                                               AS type,
                                               data_type AS return_type,
                                               routine_definition AS definition
                                        FROM information_schema.routines
                                        WHERE routine_schema NOT IN ('sys', 'information_schema',
                                                                     'mysql', 'performance_schema')
                                        AND routine_schema = '{0}'
                                        AND routine_name = '{1}';
                            ", _ebconfactoryDatadb.DBName, file_name_shrt);
                }
                EbDataTable dt = _ebconfactoryDatadb.DoQuery(str);
                if (dt != null && dt.Rows.Count > 0)
                {
                    for (int i = 0; i < dt.Rows.Count; i++)
                    {
                        result = dt.Rows[i]["definition"].ToString();
                        if (vendor == "PGSQL")
                        {
                            type = DBManagerType.Function;
                            file_name = filename;
                            result = FormatDBStringPGSQL(result, DBManagerType.Function);
                        }
                        else if (vendor == "MYSQL")
                        {
                            type = (DBManagerType)Enum.ToObject(typeof(DBManagerType), dt.Rows[i]["type"]);
                            file_name = filename;
                            result = result.Replace(" ", "").Replace("\r", "").Replace("\t", "").Replace("\n", "");
                        }

                        MD5 = StringToMD5Converter(result);
                        dictTenant.Add(file_name, new[] { vendor, MD5, type.ToString() });
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
        }

        void GetTableDetailsFromTenant(IDatabase _ebconfactoryDatadb)
        {
            string result = string.Empty;
            string file_name;
            try
            {
                Console.WriteLine("Tenant Table Details... Line No: 1274");
                string vendor = _ebconfactoryDatadb.Vendor.ToString();
                string str = null;
                if (vendor == "MYSQL")
                    str = String.Format(@"
                        SELECT DISTINCT table_name
                        FROM information_schema.tables
	                    WHERE table_name LIKE 'eb_%'
                        AND TABLE_SCHEMA = '{0}'", _ebconfactoryDatadb.DBName);
                else
                    str = @"
                        SELECT DISTINCT table_name
                        FROM information_schema.tables
	                    WHERE table_name LIKE 'eb_%'";
                EbDataTable dt1 = _ebconfactoryDatadb.DoQuery(str);
                if (dt1 != null && dt1.Rows.Count > 0)
                {
                    for (int i = 0; i < dt1.Rows.Count; i++)
                    {
                        result = GetTableScriptFromTenant(dt1.Rows[i][0].ToString(), _ebconfactoryDatadb, vendor);
                        file_name = GetFileName(result, null, DBManagerType.Table, vendor);
                        dictTenant.Add(file_name, new[] { vendor, result, DBManagerType.Table.ToString() });
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
        }

        void GetATableDetailsFromTenant(IDatabase _ebconfactoryDatadb, string filename)
        {
            string result = string.Empty;
            string file_name;
            try
            {
                string vendor = _ebconfactoryDatadb.Vendor.ToString();
                string str = null;
                if (vendor == "MYSQL")
                    str = String.Format(@"
                        SELECT DISTINCT table_name
                        FROM information_schema.tables
	                    WHERE table_name ='{0}'
                        AND TABLE_SCHEMA = '{1}'", filename, _ebconfactoryDatadb.DBName);
                else
                    str = String.Format(@"
                        SELECT DISTINCT table_name
                        FROM information_schema.tables
	                    WHERE table_name = '{0}'", filename);
                EbDataTable dt1 = _ebconfactoryDatadb.DoQuery(str);
                if (dt1 != null && dt1.Rows.Count > 0)
                {
                    for (int i = 0; i < dt1.Rows.Count; i++)
                    {
                        result = GetTableScriptFromTenant(dt1.Rows[i][0].ToString(), _ebconfactoryDatadb, vendor);
                        file_name = filename;
                        dictTenant.Add(file_name, new[] { vendor, result, DBManagerType.Table.ToString() });
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
        }

        //-------------------------------------------------------COMPARING VALUES--------------------------------------------------------

        List<Eb_FileDetails> CompareScripts(IDatabase _ebconfactoryDatadb)
        {
            List<Eb_FileDetails> ChangesList = new List<Eb_FileDetails>();
            Console.WriteLine("Tenant Compare Scriptes... Line No: 1348");
            foreach (KeyValuePair<string, Eb_FileDetails> _infraitem in dictInfra)
            {
                try
                {
                    if (dictTenant.TryGetValue(_infraitem.Key, out string[] _tenantitem)) // file name checking 
                    {
                        if ((_tenantitem != null) && (_tenantitem[1] != _infraitem.Value.Content))
                        {
                            if (_infraitem.Value.Type == DBManagerType.Table)
                            {
                                bool _isChangeinFields = false;
                                bool _isChangeinIndexs = false;
                                Eb_TableFieldChangesList infra_table_details = JsonConvert.DeserializeObject<Eb_TableFieldChangesList>(_infraitem.Value.Content);
                                Eb_TableFieldChangesList tenant_table_details = JsonConvert.DeserializeObject<Eb_TableFieldChangesList>(_tenantitem[1]);
                                _isChangeinFields = IsChangeInTableFields(infra_table_details.Fields, tenant_table_details.Fields);
                                _isChangeinIndexs = IsChangeInTableIndexs(infra_table_details.Indexs, tenant_table_details.Indexs);
                                if (_isChangeinFields == true || _isChangeinIndexs == true)
                                {
                                    ChangesList.AddRange(CreateListofChanges(_infraitem, false));
                                    _isChangeinFields = false;
                                    _isChangeinIndexs = false;
                                }
                            }
                            else
                                ChangesList.AddRange(CreateListofChanges(_infraitem, false));
                        }
                    }
                    else
                        ChangesList.AddRange(CreateListofChanges(_infraitem, true));
                }
                catch (Exception e)
                {
                    Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                    throw e;
                }
            }
            return ChangesList;
        }

        bool IsChangeInTableFields(Dictionary<string, List<Eb_TableFieldChanges>> infra_table_field_details, Dictionary<string, List<Eb_TableFieldChanges>> tenant_table_field_details)
        {
            bool _isChange = false;
            Console.WriteLine("Tenant Comparing Changes in Table Fields... Line No: 1391");
            try
            {
                if (tenant_table_field_details == null)
                    _isChange = true;
                else
                    foreach (KeyValuePair<string, List<Eb_TableFieldChanges>> _infratableitem in infra_table_field_details)
                    {
                        if (!tenant_table_field_details.TryGetValue(_infratableitem.Key, out List<Eb_TableFieldChanges> _tenanttableitem)) // checking table fields
                        {
                            _isChange = true;
                        }
                        else
                        {
                            if (_infratableitem.Value[0].Data_type != _tenanttableitem[0].Data_type)
                                _isChange = true;
                            if (_infratableitem.Value[0].Column_default != _tenanttableitem[0].Column_default)
                                _isChange = true;
                            if (_infratableitem.Value[0].Constraint_name != _tenanttableitem[0].Constraint_name)
                                _isChange = true;
                            if (_infratableitem.Value[0].Constraint_type != _tenanttableitem[0].Constraint_type)
                                _isChange = true;
                        }
                    }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return _isChange;
        }

        bool IsChangeInTableIndexs(Dictionary<string, string> infra_table_index_details, Dictionary<string, string> tenant_table_index_details)
        {
            bool _isChange = false;
            Console.WriteLine("Tenant Comparing Changes in Index... Line No: 1427");
            try
            {
                if (tenant_table_index_details == null)
                    _isChange = true;
                else
                    foreach (KeyValuePair<string, string> _infratableitem in infra_table_index_details)
                    {
                        if (!tenant_table_index_details.TryGetValue(_infratableitem.Key, out string _tenanttableitem)) // checking indexes
                        {
                            _isChange = true;
                        }
                        else
                        {
                            if (_infratableitem.Value != _tenanttableitem)
                                _isChange = true;
                        }
                    }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return _isChange;
        }

        List<Eb_FileDetails> CreateListofChanges(KeyValuePair<string, Eb_FileDetails> _infraitem, bool _newItem)
        {
            Console.WriteLine("Tenant Creating Changes List... Line No: 1456");
            List<Eb_FileDetails> ChangesList = new List<Eb_FileDetails>
            {
                new Eb_FileDetails
                {
                    Id = _infraitem.Value.Id,
                    FileHeader = _infraitem.Key,
                    FilePath = _infraitem.Value.FilePath,
                    Vendor = _infraitem.Value.Vendor,
                    Content = _infraitem.Value.Content,
                    Type = _infraitem.Value.Type,
                    FileType = Enum.GetName(typeof(DBManagerType), _infraitem.Value.Type),
                    NewItem = _newItem
                }
            };
            return ChangesList;
        }

        //----------------------------------------- MODIFY FUNCTION, PROCEDURE, TABLE ----------------------------------------------------


        //Change button click
        string UpdateDBFileByDB(List<Eb_FileDetails> ChangesList, string Solution)
        {
            DateTime modified_date = DateTime.Now;
            try
            {
                UpdateDBFilesByDBResponse resp = new UpdateDBFilesByDBResponse();
                IDatabase _ebconfactoryDatadb = GetTenantDB(Solution);
                UpdateDB(ChangesList, _ebconfactoryDatadb, Solution);
                using (DbConnection con = InfraConnectionFactory.DataDB.GetNewConnection())
                {
                    con.Open();
                    string str1 = string.Format(@"
                                              UPDATE infra_dbchangeslog 
                                              SET modified_at = NOW()
                                              WHERE solution_id = '{0}'", Solution);
                    using (DbCommand cmd2 = InfraConnectionFactory.DataDB.GetNewCommand(con, str1))
                    {
                        cmd2.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return modified_date.ToString();
        }

        void UpdateDB(List<Eb_FileDetails> ChangesList, IDatabase _ebconfactoryDatadb, string solution_id)
        {
            try
            {
                for (int i = 0; i < ChangesList.Count; i++)
                {
                    if (ChangesList[i].Type == DBManagerType.Function)
                    {
                        UpdateFunctionOrProcedureOnTenant(ChangesList[i], _ebconfactoryDatadb);
                    }
                    else if (ChangesList[i].Type == DBManagerType.Table)
                    {
                        UpdateTableOnTenant(ChangesList[i], _ebconfactoryDatadb, true, solution_id);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
        }

        //--------------------------------------Modify Function or Procedure-----------------------------------------

        void UpdateFunctionOrProcedureOnTenant(Eb_FileDetails ChangesList, IDatabase _ebconfactoryDatadb)
        {
            string Urlstart = string.Format("ExpressBase.Common.sqlscripts.{0}.", ChangesList.Vendor.ToLower());
            string path = Urlstart + ChangesList.FilePath;
            var assembly = typeof(sqlscripts).Assembly;
            string result = string.Empty;
            string str = string.Empty;
            string query = string.Empty;
            try
            {
                using (Stream stream = assembly.GetManifestResourceStream(path))
                {
                    if (stream != null)
                    {
                        using (StreamReader reader = new StreamReader(stream))
                            result = reader.ReadToEnd();
                        using (DbConnection con = _ebconfactoryDatadb.GetNewConnection())
                        {
                            con.Open();
                            string func_or_proc_drop = GetFuncOrProcDrop(result, ChangesList.FileHeader, ChangesList.Vendor);
                            if (ChangesList.Vendor == "PGSQL") // Postgress can have functions having same name with different parameters (not for MySQL )
                            {
                                query += DropFunctionsWithSameNameAndDifferentParametersPGSQL(ChangesList, _ebconfactoryDatadb);
                            }
                            if (!ChangesList.NewItem && ChangesList.Vendor == "PGSQL")
                            {
                                query += func_or_proc_drop;
                            }
                            query += result;
                            using (DbCommand cmd = _ebconfactoryDatadb.GetNewCommand(con, query))
                            {
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
        }

        string DropFunctionsWithSameNameAndDifferentParametersPGSQL(Eb_FileDetails ChangesList, IDatabase _ebconfactoryDatadb)
        {
            string query = string.Empty;
            try
            {
                string str = string.Format(@"
                                         SELECT pg_get_functiondef(oid)::text, proname 
                                         FROM pg_proc 
                                         WHERE proname = '{0}'
                                       ", ChangesList.FileHeader.Split("(")[0]);
                EbDataTable dt = _ebconfactoryDatadb.DoQuery(str);
                if (dt != null && dt.Rows.Count > 0)
                {
                    for (int j = 0; j < dt.Rows.Count; j++)
                    {
                        string func_def = dt.Rows[j][0].ToString();
                        string file_name = GetFileName(func_def, dt.Rows[j][1].ToString(), DBManagerType.Function, _ebconfactoryDatadb.Vendor.ToString()); // function name with parameters
                        if (file_name != ChangesList.FileHeader)
                            query += "DROP FUNCTION " + file_name + ";\n";
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return query;
        }

        //------------------------------------------- Modify Table ---------------------------------------------------

        string UpdateTableOnTenant(Eb_FileDetails ChangesList, IDatabase _ebconfactoryDatadb, bool isExecute, string solution_id)
        {
            string result = string.Empty;
            string changes = String.Empty;
            try
            {
                if (ChangesList.NewItem)
                {
                    changes += AddNewTableOnTenant(ChangesList.Vendor, ChangesList.FilePath, _ebconfactoryDatadb);
                }
                else
                {
                    Eb_TableFieldChangesList infra_table_details = JsonConvert.DeserializeObject<Eb_TableFieldChangesList>(ChangesList.Content);
                    Eb_TableFieldChangesList tenant_table_details = null;
                    tenant_table_details = JsonConvert.DeserializeObject<Eb_TableFieldChangesList>(GetTableScriptFromTenant(ChangesList.FileHeader, _ebconfactoryDatadb, ChangesList.Vendor));
                    changes += DropExtraTableIndexOnTenant(infra_table_details, tenant_table_details, ChangesList.Vendor);
                    changes += CheckChangesInTableFieldsOnTenant(infra_table_details, tenant_table_details, _ebconfactoryDatadb, ChangesList.Vendor, ChangesList.FilePath);
                    changes += CheckChangesInTableIndexOnTenant(infra_table_details, tenant_table_details, ChangesList.Vendor);
                    changes = changes.Trim().Replace("  ", "").Replace(";", ";\n");
                    if (isExecute)
                    {
                        ExecuteQuery(changes, _ebconfactoryDatadb, solution_id);
                        changes = string.Empty;
                    }
                    else
                        return changes;

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return changes;
        }

        //--------------------------------------Create New Table If dont Exist ---------------------------------------

        string AddNewTableOnTenant(string vendor, string filepath, IDatabase _ebconfactoryDatadb)
        {
            string result = string.Empty;
            string Urlstart = string.Format("ExpressBase.Common.sqlscripts.{0}.", vendor.ToLower());
            string path = Urlstart + filepath;
            var assembly = typeof(sqlscripts).Assembly;
            try
            {
                using (Stream stream = assembly.GetManifestResourceStream(path))
                {
                    if (stream != null)
                    {
                        using (StreamReader reader = new StreamReader(stream))
                            result = reader.ReadToEnd();
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return result;
        }

        //-------------------------------------------------FIELDS-----------------------------------------------------

        //--------------------------------
        string GetDataTypeSizeForMySql(IDatabase _ebconfactoryDatadb, string file_path, string column_name, string data_type, string table_name, string column_default, string column_constraint_type)
        {
            string result;
            string size = string.Empty;
            try
            {
                string Urlstart = string.Format("ExpressBase.Common.sqlscripts.mysql.");
                string path = Urlstart + file_path;
                var assembly = typeof(sqlscripts).Assembly;
                if (data_type == "text")
                {
                    if (column_default != "")
                        data_type = "varchar";
                    if (column_constraint_type == "UNIQUE")
                        data_type = "varchar";
                    data_type = IndexCheckForMysqlDatatype(_ebconfactoryDatadb, data_type, column_name, table_name);
                }

                using (Stream stream = assembly.GetManifestResourceStream(path))
                {
                    if (stream != null)
                    {
                        using (StreamReader reader = new StreamReader(stream))
                            result = reader.ReadToEnd();
                        string val = column_name + " " + data_type;
                        if (result.Contains(val + "("))
                        {
                            Regex regex = new Regex(val + @"\(.*?\)");
                            MatchCollection matches = regex.Matches(result);
                            size = matches[0].ToString().Replace(val, "");
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return size;
        }

        //----------------------------------Check Changes in Table Fields to Modify-----------------------------------

        string CheckChangesInTableFieldsOnTenant(Eb_TableFieldChangesList infra_table_details, Eb_TableFieldChangesList tenant_table_details, IDatabase _ebconfactoryDatadb, string vendor, string file_path)
        {
            string changes = String.Empty;
            string data_type_size = String.Empty;
            try
            {
                foreach (KeyValuePair<string, List<Eb_TableFieldChanges>> _infratableitem in infra_table_details.Fields)
                {
                    if (vendor == "MYSQL")
                        data_type_size = GetDataTypeSizeForMySql(_ebconfactoryDatadb, file_path, _infratableitem.Key, _infratableitem.Value[0].Data_type, infra_table_details.Table_name, _infratableitem.Value[0].Column_default, _infratableitem.Value[0].Constraint_type);
                    if (!tenant_table_details.Fields.TryGetValue(_infratableitem.Key, out List<Eb_TableFieldChanges> _tenanttableitem))
                    {
                        changes += AddNewColumnsOnTenant(infra_table_details.Table_name, _infratableitem.Key, _infratableitem.Value[0].Data_type, _infratableitem.Value[0].Column_default, _infratableitem.Value[0].Constraint_name, _infratableitem.Value[0].Constraint_type, data_type_size, _ebconfactoryDatadb);
                    }
                    else
                    {
                        changes += ModifyColumnsOnTenant(_infratableitem.Value[0].Data_type, _tenanttableitem[0].Data_type, infra_table_details.Table_name, _infratableitem.Key, _infratableitem.Value[0].Constraint_type, _tenanttableitem[0].Constraint_type, _infratableitem.Value[0].Constraint_name, _tenanttableitem[0].Constraint_name, _infratableitem.Value[0].Column_default, _tenanttableitem[0].Column_default, vendor, data_type_size, _ebconfactoryDatadb);
                    }
                    //string str1 = string.Format(@"
                    //                            ALTER TABLE {0} DROP COLUMN {1}", infra_table_dict.Table_name, _infratableitem.Key);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return changes;
        }

        //------------------------------------Create New Column If Don't Exist----------------------------------------

        string AddNewColumnsOnTenant(string infra_table_name, string infra_column, string infra_column_data_type, string infra_column_default, string infra_column_constraint_name, string infra_column_constraint_type, string data_type_size, IDatabase _ebconfactoryDatadb)
        {
            string changes = String.Empty;
            try
            {
                if (_ebconfactoryDatadb.Vendor.ToString() == "MYSQL" && infra_column_data_type == "text")
                {
                    if (infra_column_default != "")
                        infra_column_data_type = "varchar";
                    if (infra_column_constraint_type == "UNIQUE")
                        infra_column_data_type = "varchar";
                    infra_column_data_type = IndexCheckForMysqlDatatype(_ebconfactoryDatadb, infra_column_data_type, infra_column, infra_table_name);
                }

                if (data_type_size != "")
                    infra_column_data_type = infra_column_data_type + data_type_size;
                changes = changes + string.Format(@"
                                                ALTER TABLE {0} 
                                                ADD COLUMN {1} {2};
                                              ", infra_table_name, infra_column, infra_column_data_type);

                if (infra_column_default != "")
                {
                    changes = changes + string.Format(@"
                                                    ALTER TABLE {0}
                                                    ALTER COLUMN {1}
                                                    SET DEFAULT {2};
                                                  ", infra_table_name, infra_column, infra_column_default); // set default and set not null
                }
                if (infra_column_constraint_type == "UNIQUE")
                {
                    changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    ADD CONSTRAINT {1} UNIQUE ({2});
                                                    ", infra_table_name, infra_column_constraint_name, infra_column);
                }
                else if (infra_column_constraint_type == "PRIMARY KEY")
                {
                    changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    ADD PRIMARY KEY ({1});
                                                    ", infra_table_name, infra_column);
                }
                else if (infra_column_constraint_type == "CHECK")
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
                                             ", infra_column_constraint_name);
                    EbDataTable dt1 = InfraConnectionFactory.DataDB.DoQuery(str1);
                    if (dt1 != null && dt1.Rows.Count > 0)
                    {
                        string expression = dt1.Rows[0][0].ToString();
                        changes = changes + string.Format(@"
                                                        ALTER TABLE {0} 
                                                        ADD CONSTRAINT {1} CHECK {2};
                                                       ", infra_table_name, infra_column_constraint_name, infra_column, expression);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }

            //Check Primary key already exist or not
            //Foreign Key 
            return changes;
        }

        //-----------------------------------Check Index For MySql (text datatype)------------------------------------

        string IndexCheckForMysqlDatatypeInPostgres(string data_type, string column_name, string table_name)
        {
            string str = String.Format(@"select indexdef from pg_indexes where tablename = '{0}'", table_name);
            try
            {
                EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
                if (dt.Rows.Count > 0)
                {
                    for (int i = 0; i < dt.Rows.Count; i++)
                    {
                        if (dt.Rows[i][0].ToString().Contains("(" + column_name + ")"))
                            data_type = "varchar";
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return data_type;
        }

        string IndexCheckForMysqlDatatype(IDatabase _ebconfactoryDatadb, string data_type, string column_name, string table_name)
        {
            string str = String.Format(@"SHOW INDEX FROM {0} 
                                   WHERE column_name='{1}'", table_name, column_name);
            try
            {
                EbDataTable dt = _ebconfactoryDatadb.DoQuery(str);
                if (dt.Rows.Count > 0)
                    data_type = "varchar";
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return data_type;
        }

        //------------------------------------------Modify Table Columns----------------------------------------------

        string ModifyColumnsOnTenant(string infra_column_data_type, string tenant_column_data_type, string infra_table_name, string infra_column_name, string infra_constraint_type, string tenant_constraint_type, string infra_constraint_name, string tenant_constraint_name, string infra_column_default, string tenant_column_default, string vendor, string data_type_size, IDatabase _ebconfactoryDatadb)
        {
            string changes = String.Empty;
            try
            {
                if (infra_column_data_type != tenant_column_data_type)
                {

                    if (data_type_size != "")
                        infra_column_data_type = infra_column_data_type + data_type_size;
                    if (vendor == "PGSQL")
                        changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    ALTER COLUMN {1} TYPE {2};", infra_table_name, infra_column_name, infra_column_data_type);
                    else if (vendor == "MYSQL")
                    {
                        if (infra_column_data_type == "text")
                        {
                            if (infra_column_default != "")
                                infra_column_data_type = "varchar";
                            if (infra_constraint_type == "UNIQUE")
                                infra_column_data_type = "varchar";
                            infra_column_data_type = IndexCheckForMysqlDatatypeInPostgres(infra_column_data_type, infra_column_name, infra_table_name);
                        }
                        changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    MODIFY {1} {2};", infra_table_name, infra_column_name, infra_column_data_type);
                    }

                }
                if (infra_constraint_type != tenant_constraint_type)
                {
                    changes += ModifyColumnConstraintTypeOnTenant(infra_constraint_type, infra_table_name, tenant_constraint_name, infra_constraint_name, infra_column_name, tenant_constraint_type, _ebconfactoryDatadb);
                }
                if (infra_column_default != tenant_column_default)
                {
                    changes += ModifyColumnDefaultOnTenant(infra_column_default, tenant_column_default, infra_table_name, infra_column_name, _ebconfactoryDatadb);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return changes;
        }

        //----------------------------------------Modify Column Constraint--------------------------------------------

        string ModifyColumnConstraintTypeOnTenant(string infra_constraint_type, string infra_table_name, string tenant_constraint_name, string infra_constraint_name, string infra_column_name, string tenant_constraint_type, IDatabase _ebconfactoryDatadb)
        {
            string changes = String.Empty;
            string vendor = _ebconfactoryDatadb.Vendor.ToString();
            try
            {
                if (infra_constraint_type == null || infra_constraint_type == "")
                {
                    changes += DropConstraintSqlScripts(vendor, infra_table_name, tenant_constraint_name, tenant_constraint_type);
                }
                else if (infra_constraint_type == "UNIQUE")
                {
                    changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    ADD CONSTRAINT {1} UNIQUE ({2});
                                                    ", infra_table_name, infra_constraint_name, infra_column_name);
                }
                else if (infra_constraint_type == "CHECK")
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
                                                 ", infra_constraint_name);
                    EbDataTable dt1 = InfraConnectionFactory.DataDB.DoQuery(str1);
                    if (dt1 != null && dt1.Rows.Count > 0)
                    {
                        string expression = dt1.Rows[0][0].ToString();
                        changes = changes + string.Format(@"
                                                            ALTER TABLE {0} 
                                                            ADD CONSTRAINT {1} CHECK {2};
                                                          ", infra_table_name, infra_constraint_name, expression);
                    }
                }
                else if (infra_constraint_type == "PRIMARY KEY")
                {
                    if (tenant_constraint_type == "")
                    {
                        changes = changes + string.Format(@"
                                                            ALTER TABLE {0} 
                                                            ADD PRIMARY KEY ({1});
                                                          ", infra_table_name, infra_column_name);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return changes;
        }

        //---------------------------------Get Drop Scripts For Column Constraints------------------------------------

        string DropConstraintSqlScripts(string vendor, string infra_table_name, string tenant_constraint_name, string tenant_constraint_type)
        {
            string changes = String.Empty;
            if (vendor == "PGSQL")
                changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    DROP CONSTRAINT {1};
                                                    ", infra_table_name, tenant_constraint_name);
            else if (vendor == "MYSQL")
            {
                if (tenant_constraint_type == "UNIQUE")
                    changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    DROP INDEX {1};
                                                    ", infra_table_name, tenant_constraint_name);
                if (tenant_constraint_type == "PRIMARY KEY")
                    changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    DROP PRIMARY KEY;
                                                    ", infra_table_name);
                if (tenant_constraint_type == "FOREIGN KEY")
                    changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    DROP FOREIGN KEY {1};
                                                    ", infra_table_name, tenant_constraint_name);
                if (tenant_constraint_type == "CHECK ")
                    changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    DROP CHECK {1};
                                                    ", infra_table_name, tenant_constraint_name);
            }
            return changes;
        }

        //------------------------------------------Modify Column Default---------------------------------------------

        string ModifyColumnDefaultOnTenant(string infra_column_default, string tenant_column_default, string infra_table_name, string infra_column_name, IDatabase _ebconfactoryDatadb)
        {
            string changes = String.Empty;
            string vendor = _ebconfactoryDatadb.Vendor.ToString();
            try
            {
                if (infra_column_default == null || infra_column_default == "")
                {
                    changes += DropColumnDefault(vendor, infra_table_name, infra_column_name);
                }
                else
                {
                    if (infra_column_default.Contains("nextval"))
                    {
                        changes += ModifySequenceForTable(infra_column_default, tenant_column_default, infra_column_name, infra_table_name, _ebconfactoryDatadb);
                    }
                    else
                    {
                        if (!int.TryParse(infra_column_default, out int x) && infra_column_default != "'F'::\"char\"" && infra_column_default != "'F'::bpchar" && infra_column_default != "'default'::text")
                            infra_column_default = "'" + infra_column_default + "'";
                        if (vendor == "PGSQL")
                            changes = changes + string.Format(@"
                                                   ALTER TABLE {0} ALTER COLUMN {1} SET DEFAULT {2};
                                                    ", infra_table_name, infra_column_name, infra_column_default);
                        else if (vendor == "MYSQL")
                            changes = changes + string.Format(@"
                                                   ALTER TABLE {0} ALTER {1} SET DEFAULT {2};
                                                    ", infra_table_name, infra_column_name, infra_column_default);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return changes;
        }

        //-------------------------------------------Drop Column Default----------------------------------------------

        string DropColumnDefault(string vendor, string infra_table_name, string infra_column_name)
        {
            string changes = String.Empty;
            if (vendor == "PGSQL")
                changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    ALTER COLUMN {1} DROP DEFAULT;
                                                    ", infra_table_name, infra_column_name);
            else if (vendor == "MYSQL")
                changes = changes + string.Format(@"
                                                    ALTER TABLE {0} 
                                                    ALTER {1} DROP DEFAULT;
                                                    ", infra_table_name, infra_column_name);
            return changes;
        }

        //--------------------------------------------Modify Sequences------------------------------------------------

        string ModifySequenceForTable(string infra_column_default, string tenant_column_default, string infra_column_name, string infra_table_name, IDatabase _ebconfactoryDatadb)
        {
            string changes = String.Empty;
            try
            {
                string infra_seq_name = infra_column_default.Split("'")[1];
                string tenant_seq_name = tenant_column_default.Split("'")[1];
                string str = string.Format(@"
                                                    SELECT COUNT(*) 
                                                    FROM information_schema.sequences 
                                                    WHERE sequence_name='{0}'", infra_seq_name);
                EbDataTable dt = _ebconfactoryDatadb.DoQuery(str);
                if (dt != null)
                {
                    if (dt.Rows.Count == 0)
                    {
                        changes = changes + string.Format(@"
                                                                   CREATE SEQUENCE IF NOT EXISTS {0};
                                                                   SELECT setval('{0}', (SELECT max({1})+1 FROM {2}), false);
                                                                   ALTER TABLE {2} ALTER COLUMN {1} SET DEFAULT nextval('{0}');
                                                                ", infra_seq_name, infra_column_name, infra_table_name);
                    }
                    else if (dt.Rows.Count > 0)
                    {
                        changes = changes + string.Format(@"
                                                                   DROP SEQUENCE {0};
                                                                   ALTER SEQUENCE {1} RENAME TO {0};
                                                                 ", infra_seq_name, tenant_seq_name);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return changes;
        }

        //---------------------------------------------------INDEX----------------------------------------------------

        //-----------------------------------------Changes in Table Index---------------------------------------------

        string DropExtraTableIndexOnTenant(Eb_TableFieldChangesList infra_table_details, Eb_TableFieldChangesList tenant_table_details, string vendor)
        {
            string changes = String.Empty;
            try
            {
                foreach (KeyValuePair<string, string> _tenanttableitem in tenant_table_details.Indexs)
                {
                    if (!infra_table_details.Indexs.TryGetValue(_tenanttableitem.Key, out string _infratableitem))
                    {
                        if (vendor == "PGSQL")
                            changes = changes + string.Format(@"DROP INDEX {0};", _tenanttableitem.Key);
                        else if (vendor == "MYSQL")
                            changes = changes + string.Format(@"DROP INDEX {1} ON {0};", tenant_table_details.Table_name, _tenanttableitem.Key);
                    }

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return changes;
        }

        string CheckChangesInTableIndexOnTenant(Eb_TableFieldChangesList infra_table_details, Eb_TableFieldChangesList tenant_table_details, string vendor)
        {
            string changes = String.Empty;
            try
            {
                foreach (KeyValuePair<string, string> _infratableitem in infra_table_details.Indexs)
                {
                    if (!tenant_table_details.Indexs.TryGetValue(_infratableitem.Key, out string _tenanttableitem))
                    {
                        string str = _infratableitem.Value;
                        if (vendor == "MYSQL")
                            str = _infratableitem.Value.Replace("using btree", "");
                        changes = changes + str + ";";
                    }
                    else
                    {
                        if (_infratableitem.Value != _tenanttableitem)
                        {
                            if (vendor == "PGSQL")
                                changes = changes + string.Format(@"DROP INDEX {0};", _infratableitem.Key) + _infratableitem.Value + ";";
                            else if (vendor == "MYSQL")
                                changes = changes + string.Format(@"DROP INDEX {1} ON {0};", infra_table_details.Table_name, _infratableitem.Key) + _infratableitem.Value.Replace("using btree", "") + ";";
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return changes;
        }

        //-------------------------------------------------------------------------------------------------------------------------

        string GetFunctionOrProcedureQuery(Eb_FileDetails ChangesList, IDatabase _ebconfactoryDatadb)
        {
            string Urlstart = string.Format("ExpressBase.Common.sqlscripts.{0}.", ChangesList.Vendor.ToLower());
            string path = Urlstart + ChangesList.FilePath;
            var assembly = typeof(sqlscripts).Assembly;
            string result = string.Empty;
            string str = string.Empty;
            string query = string.Empty;
            try
            {
                using (Stream stream = assembly.GetManifestResourceStream(path))
                {
                    if (stream != null)
                    {
                        using (StreamReader reader = new StreamReader(stream))
                            result = reader.ReadToEnd();
                        string func_or_proc_drop = GetFuncOrProcDrop(result, ChangesList.FileHeader, ChangesList.Vendor);
                        if (ChangesList.Vendor == "PGSQL") // Postgress can have functions having same name with different parameters (not for MySQL )
                        {
                            query += DropFunctionsWithSameNameAndDifferentParametersPGSQL(ChangesList, _ebconfactoryDatadb);
                            if (!ChangesList.NewItem)
                            {
                                query += func_or_proc_drop + "\n";
                            }
                            Regex regex = new Regex(@"CREATE(.|\n)*\$BODY\$;");
                            MatchCollection matches = regex.Matches(result);
                            if (matches.Count > 0)
                                result = matches[0].ToString();
                            else
                                result = "";
                        }
                        query += result;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return query;
        }

        string ExecuteQuery(string Query, IDatabase _ebconfactoryDatadb, string solution_id)
        {
            string Message = string.Empty;
            try
            {
                using (DbConnection con = _ebconfactoryDatadb.GetNewConnection())
                {
                    con.Open();
                    using (DbCommand cmd = _ebconfactoryDatadb.GetNewCommand(con, Query))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    con.Close();
                }
                using (DbConnection con1 = this.InfraConnectionFactory.DataDB.GetNewConnection())
                {
                    con1.Open();
                    string str1 = string.Format(@"
                                              UPDATE infra_dbchangeslog 
                                              SET modified_at = NOW()
                                              WHERE solution_id = '{0}'", solution_id);
                    using (DbCommand cmd1 = InfraConnectionFactory.DataDB.GetNewCommand(con1, str1))
                    {
                        cmd1.ExecuteNonQuery();
                    }
                    con1.Close();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                Message = e.Message;
                return Message;
                throw e;
            }
            return Message;
        }

        //-------------------------------------------------------------------------------------------------------------------------
        //-----------------------------------------------------DIFF----------------------------------------------------------------

        List<string> GetScriptContentForDiffView(string filename, string filepath, string filetype, IDatabase _ebconfactoryDatadb)
        {
            string infra_content = string.Empty;
            string tenant_file_content = string.Empty;
            List<string> result = new List<string>();
            try
            {
                if (filetype == "Table")
                {
                    infra_content = GetJSONFromInfra(filename, _ebconfactoryDatadb.Vendor.ToString());
                    tenant_file_content = FormatJSON(GetTableScriptFromTenant(filename, _ebconfactoryDatadb, _ebconfactoryDatadb.Vendor.ToString()));
                }
                else
                {
                    infra_content = GetFuncOrProcCreateScriptFromInfra(filename, filepath, filetype, _ebconfactoryDatadb.Vendor.ToString());
                    tenant_file_content = GetFuncorProcCreateScriptFromTenant(filename, filepath, filetype, _ebconfactoryDatadb);
                }
                result.Add(infra_content);
                result.Add(tenant_file_content);
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return result;
        }

        string GetJSONFromInfra(string table_name, string vendor)
        {
            string json = string.Empty;
            try
            {
                string str = string.Format(@"
                        SELECT d.contents
                        FROM infra_dbmd5 as d, infra_dbstructure as c
                        WHERE d.filename = '{0}'
                        AND d.eb_del = 'F'
						AND c.id = d.change_id
						AND c.vendor = '{1}'", table_name, vendor);
                EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
                if (dt != null && dt.Rows.Count > 0)
                {
                    json = FormatJSON(dt.Rows[0]["contents"].ToString().Replace("''", "'"));
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return json;
        }

        string GetFuncOrProcCreateScriptFromInfra(string filename, string filepath, string filetype, string vendor)
        {
            string result = string.Empty;
            try
            {
                string Urlstart = string.Format("ExpressBase.Common.sqlscripts.{0}.", vendor.ToLower());
                string path = Urlstart + filepath;
                var assembly = typeof(sqlscripts).Assembly;
                using (Stream stream = assembly.GetManifestResourceStream(path))
                {
                    if (stream != null)
                    {
                        using (StreamReader reader = new StreamReader(stream))
                            result = reader.ReadToEnd();
                        if (vendor == "PGSQL")
                            result = FormatFuncStringPGSQL(result, DBManagerType.Function, filename); //remove escape sequences, spaces
                        else if (vendor == "MYSQL")
                        {
                            if (filetype == "Function")
                                result = FormatFuncOrProcStringMYSQL(result, DBManagerType.Function, filename); //remove escape sequences, spaces
                            else if (filetype == "Procedure")
                                result = FormatFuncOrProcStringMYSQL(result, DBManagerType.Procedure, filename); //remove escape sequences, spaces
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return result;
        }

        string GetFuncorProcCreateScriptFromTenant(string filename, string filepath, string filetype, IDatabase _ebconfactoryDatadb)
        {
            string result = string.Empty;
            string str = string.Empty;
            string file_name = string.Empty;
            try
            {
                string vendor = _ebconfactoryDatadb.Vendor.ToString();
                if (vendor == "PGSQL")
                {
                    str = String.Format(@"
                            SELECT pg_get_functiondef(oid)::text as definition, proname as routine_name
                            FROM pg_proc 
                            WHERE proname 
                            IN 
                                (SELECT routine_name 
                                FROM information_schema.routines 
                                WHERE routine_type = 'FUNCTION' 
                                    AND specific_schema = 'public'
                                    AND routine_name ='{0}')", filename.Split("(")[0]);
                    EbDataTable dt = _ebconfactoryDatadb.DoQuery(str);
                    if (dt != null && dt.Rows.Count > 0)
                    {
                        result = FormatDBStringPGSQLForDiffView(dt.Rows[0]["definition"].ToString(), DBManagerType.Function);
                    }
                }
                else if (vendor == "MYSQL")
                {
                    filetype = filetype.ToUpper();
                    str = String.Format(@"
                                                SHOW CREATE {0} {1}
                                            ", filetype, filename.Split("(")[0]);
                    EbDataTable dt = _ebconfactoryDatadb.DoQuery(str);
                    if (dt != null && dt.Rows.Count > 0)
                    {
                        result = FormatDBStringMYSQLForDiffView(dt.Rows[0][2].ToString());
                    }
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return result;
        }

        //-------------------------------------------------------------------------------------------------------------------------

        string GetFuncOrProcDrop(string str, string filename, string vendor)
        {
            try
            {
                if (vendor == "PGSQL")
                {
                    Regex regex = new Regex(@"DROP.*\);");
                    MatchCollection matches = regex.Matches(str);
                    if (matches.Count > 0)
                        str = matches[0].ToString();
                    else
                        str = "DROP FUNCTION " + filename;
                }
                else if (vendor == "MYSQL")
                {

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace + "File Name : " + filename);
                throw e;
            }
            return str;
        }

        string GetFileName(string str, string file, DBManagerType type, string vendor)
        {
            string res = string.Empty;
            try
            {
                if (type == DBManagerType.Function)
                {
                    if (vendor == "PGSQL")
                        res = PGSQLGetFunctionName(str, file);
                    if (vendor == "MYSQL")
                        res = MYSQLGetFunctionName(str);
                }
                else if (type == DBManagerType.Table && file != null)
                {
                    if (vendor == "PGSQL")
                        res = file.Split(".")[2];
                    //res = PGSQLGetTableNameFromSQLScripts(str, file);
                    if (vendor == "MYSQL")
                        res = file.Split(".")[2];
                }
                else if (type == DBManagerType.Table && file == null)
                {
                    if (vendor == "PGSQL" || vendor == "MYSQL")
                        res = PGSQLGetTableNameFromJSON(str);
                }
                else if (type == DBManagerType.Procedure)
                {
                    if (vendor == "MYSQL")
                        res = MYSQLGetProcedureName(str);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return res;
        }

        string PGSQLGetFunctionName(string str, string file)
        {
            string[] fname;
            string res = string.Empty;
            try
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
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return res.Replace(", ", ",").Trim();
        }

        string PGSQLGetTableNameFromSQLScripts(string str, string file)
        {
            string res = string.Empty;
            try
            {
                int x = file.Split(".").Length;
                res = str.Split(" ").Length > 1 ? str.Split(" ")[2] : file.Split(".")[file.Split(".").Length - 2];
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return res.Remove(0, 7).Replace("\r", "").Replace("\n", "").Replace("--", "");
        }

        string MYSQLGetTableNameFromSQLScripts(string str, string file)
        {
            string res = string.Empty;
            try
            {
                int x = file.Split(".").Length;
                res = str.Split(" ").Length > 1 ? str.Split(" ")[2] : file.Split(".")[file.Split(".").Length - 2];
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return res.Remove(0, 7).Replace("\r", "").Replace("\n", "").Replace("--", "");
        }

        string PGSQLGetTableNameFromJSON(string str)
        {
            return str.Split("\"")[3];
        }

        string MYSQLGetFunctionName(string str)
        {
            string res = string.Empty;
            Regex regex = new Regex(@"([a-zA-Z]+(?:_[a-zA-Z0-9]+)*)");
            MatchCollection matches = regex.Matches(str);
            if (matches.Count > 4)
            {
                res = matches[4].Value.ToString();
            }
            else
            {
                Console.WriteLine("MYSQLGetFunctionName.matches.string:");
            }
            return res;
        }

        string MYSQLGetProcedureName(string str)
        {
            string res = string.Empty;
            Regex regex = new Regex(@"([a-zA-Z]+(?:_[a-zA-Z0-9]+)*)");
            MatchCollection matches = regex.Matches(str);
            return matches[4].Value.ToString();
        }

        string FormatDBStringPGSQL(string str, DBManagerType type)
        {
            if (type == DBManagerType.Function)
            {
                str = str.Replace(" ", "").Replace("\r", "").Replace("\t", "").Replace("\n", "").Trim();
            }
            return str;
        }

        string FormatFileStringPGSQL(string str, DBManagerType type, string filename)
        {
            try
            {
                if (type == DBManagerType.Function)
                {
                    str = str.Replace("$BODY$", "$function$");
                    Regex regex = new Regex(@"CREATE(.|\n)*\$function\$;");
                    MatchCollection matches = regex.Matches(str);
                    if (matches.Count > 0)
                        str = matches[0].ToString().Replace("'plpgsql'", "plpgsql").Replace(" ", "").Replace("\t", "").Replace("\n", "").Replace("$function$;", "$function$").Replace("\r", "");
                    else
                        str = "";
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace + "File Name : " + filename);
                throw e;
            }
            return str;
        }

        string FormatFileStringMYSQL(string str, DBManagerType type, string filename)
        {
            try
            {
                if (type == DBManagerType.Procedure || type == DBManagerType.Function)
                {
                    string drop = str.Split("BEGIN")[0];
                    str = str.Replace(drop, "");
                    str = str.Replace(" ", "").Replace("\r", "").Replace("\t", "").Replace("\n", "").Trim();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace + "File Name : " + filename);
                throw e;
            }
            return str;
        }

        string GetFuncDef(string str)
        {
            if (str.Contains("\r"))
                str = str.Replace("\r", "");
            string[] split = str.Split("\n\n");
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

        DBManagerType CheckFunctionOrProcedure(string file_content)
        {
            DBManagerType type = DBManagerType.Function;
            if (file_content.Contains("FUNCTION"))
                type = DBManagerType.Function;
            else if (file_content.Contains("PROCEDURE"))
                type = DBManagerType.Procedure;
            return type;
        }

        string FormatJSON(string json)
        {
            try
            {
                json = JValue.Parse(json).ToString(Formatting.Indented);
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return json;
        }

        string FormatFuncStringPGSQL(string str, DBManagerType type, string filename)
        {
            try
            {
                if (type == DBManagerType.Function)
                {
                    str = str.Replace("$BODY$", "$function$");
                    Regex regex = new Regex(@"CREATE(.|\n)*\$function\$;");
                    MatchCollection matches = regex.Matches(str);
                    if (matches.Count > 0)
                    {
                        str = matches[0].ToString().Replace("'plpgsql'", "plpgsql").Replace("$function$;", "$function$").Replace("\r", "");
                        str = str.Replace("  ", "").Replace("\t", "").Replace(",\n", ", ").Replace("RETURNS", " RETURNS").Replace("LANGUAGE", " LANGUAGE").Trim();
                    }
                    else
                        str = "";
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace + "File Name : " + filename);
                throw e;
            }
            return str;
        }

        string FormatFuncOrProcStringMYSQL(string str, DBManagerType type, string filename)
        {
            try
            {
                if (type == DBManagerType.Procedure || type == DBManagerType.Function)
                {
                    if (str.Contains("\r"))
                        str = str.Replace("\r", "");
                    string drop = str.Split("\n\n")[0];
                    str = str.Replace(drop, "").Trim();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace + "File Name : " + filename);
                throw e;
            }
            return str;
        }

        string FormatDBStringPGSQLForDiffView(string str, DBManagerType type)
        {
            try
            {
                if (type == DBManagerType.Function)
                {
                    Regex regex = new Regex(@"\(.[a-z]*");
                    MatchCollection matches = regex.Matches(str);
                    string x = matches[0].ToString().Replace("(", "");
                    string s = "(\n" + x;
                    str = str.Replace("  ", "").Replace("\r", "").Replace(",\n", ",").Replace("\t", "").Replace(matches[0].ToString(), s).Trim();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e.Message + " : " + e.StackTrace);
                throw e;
            }
            return str;
        }

        string FormatDBStringMYSQLForDiffView(string str)
        {
            Regex regex = new Regex(@"DEFINER=[`a-zA-Z@%]* ");
            MatchCollection matches = regex.Matches(str);
            str = str.Replace(matches[0].ToString(), "").Replace("`", "");
            return str;
        }

        [Authenticate]
        public void Post(LastSolnAccessRequest request)
        {
            try
            {
                EbDataTable solutionlist = SelectSolutionsFromDB();
                List<LastDbAccess> LastDbAccessList = new List<LastDbAccess>();
                for (int i = 0; i < solutionlist.Rows.Count; i++)
                {
                    try
                    {
                        string i_solution_id = solutionlist.Rows[i]["isolution_id"].ToString();
                        EbConnectionsConfig Conf = this.Redis.Get<EbConnectionsConfig>(string.Format(CoreConstants.SOLUTION_INTEGRATION_REDIS_KEY, i_solution_id));

                        if (Conf != null && Conf.DataDbConfig != null)
                        {
                            EbConnectionFactory factory = new EbConnectionFactory(Conf, i_solution_id);
                            LastDbAccess access = new LastDbAccess
                            {
                                ISolution = i_solution_id,
                                ESolution = solutionlist.Rows[i]["esolution_id"].ToString() + ", " + solutionlist.Rows[i]["date_created"].ToString(),
                                AdminUserName = Conf.DataDbConfig.UserName,
                                AdminPassword = Conf.DataDbConfig.Password,
                                ReadWriteUserName = Conf.DataDbConfig.ReadWriteUserName,
                                ReadWritePassword = Conf.DataDbConfig.ReadWritePassword,
                                ReadOnlyUserName = Conf.DataDbConfig.ReadOnlyUserName,
                                ReadOnlyPassword = Conf.DataDbConfig.ReadOnlyPassword,
                                DbName = Conf.DataDbConfig.DatabaseName,
                                DbVendor = Conf.DataDbConfig.DatabaseVendor.ToString()
                            };

                            try
                            {
                                string query = @"   
                                SELECT display_name, commit_ts, (select count(*) from eb_objects)
                                FROM 
                                    eb_objects O, eb_objects_ver v 
                                WHERE 
                                    v.eb_objects_id = o.id AND commit_ts = (SELECT MAX(commit_ts) FROM eb_objects_ver);

                                SELECT fullname ,(select count(*) from eb_users)
                                FROM 
                                    eb_users 
                                WHERE id =(SELECT MAX(id) FROM eb_users);

                                SELECT fullname, signin_at 
                                FROM
                                    eb_signin_log s, eb_users u 
                                WHERE s.id = (SELECT MAX(id) FROM eb_signin_log)
                                    AND user_id = u.id;

                                SELECT string_agg(applicationname,',') , (select count(*) from eb_applications)
                                FROM 
                                    eb_applications WHERE COALESCE(eb_del,'F')='F'
                                ";
                                EbDataSet ds = factory.DataDB.DoQueries(query);
                                if (ds.Tables[0] != null && ds.Tables[0].Rows != null && ds.Tables[0].Rows.Count > 0)
                                {
                                    access.LastObject = ds.Tables[0].Rows[0][0].ToString() + ", " + ds.Tables[0].Rows[0][1].ToString();
                                    access.ObjCount = ds.Tables[0].Rows[0][2].ToString();
                                }
                                if (ds.Tables[1] != null && ds.Tables[1].Rows != null && ds.Tables[1].Rows.Count > 0)
                                {
                                    access.LastUser = ds.Tables[1].Rows[0][0].ToString();
                                    access.UsrCount = ds.Tables[1].Rows[0][1].ToString();
                                }
                                if (ds.Tables[2] != null && ds.Tables[2].Rows != null && ds.Tables[2].Rows.Count > 0)
                                {
                                    access.LastLogin = ds.Tables[2].Rows[0][0].ToString() + ", " + ds.Tables[2].Rows[0][1].ToString();
                                }
                                if (ds.Tables[3] != null && ds.Tables[3].Rows != null && ds.Tables[3].Rows.Count > 0)
                                {
                                    access.Applications = ds.Tables[3].Rows[0][0].ToString();
                                    access.AppCount = ds.Tables[3].Rows[0][1].ToString();
                                }

                                LastDbAccessList.Add(access);
                            }
                            catch (Exception e)
                            {
                                LastDbAccessList.Add(access);
                                Console.WriteLine("Error in LastSolnAccessRequest. Isolnid:-i_solution_id" + i_solution_id + "  " + e.Message);
                            }

                        }
                    }
                    catch (Exception e)
                    {

                        Console.WriteLine(e.Message + e.StackTrace);
                        continue;
                    }
                }
                string csv = LastDbAccessList.ToCsv();
                string Q2 = "INSERT INTO eb_lastaccess_csv(csv, created_at) VALUES(:csv, NOW())";
                DbParameter[] _params = { this.InfraConnectionFactory.DataDB.GetNewParameter("csv", Common.Structures.EbDbTypes.String, csv) };
                this.InfraConnectionFactory.DataDB.DoNonQuery(Q2, _params);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception in LastSolnAccessRequest" + e.Message + e.StackTrace);
            }
        }
    }
}