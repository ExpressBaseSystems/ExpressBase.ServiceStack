using ExpressBase.Common;
using ExpressBase.Common.Data;
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

        IDictionary<string, string[]> dictTenant = new Dictionary<string, string[]>();
        IDictionary<string, string[]> dictInfra = new Dictionary<string, string[]>();
        IDictionary<string, string[]> dictToDB = new Dictionary<string, string[]>();

        public void Post(FunctionCheckRequest request)
        {
            try
            {
                //EbConnectionFactory _ebConnectionFactory;
                //foreach (string name in SolNames)
                //    if (name != string.Empty)
                //    {
                //        try
                //        {
                //            _ebConnectionFactory = new EbConnectionFactory(name, Redis);

                //        }
                //        catch(Exception e)
                //        {
                //            Console.WriteLine(e.Message);
                //            continue;
                //        }
                //    }
                //    else
                //    {
                //        int a = 0;
                //    }

                //SetFuncMd5InfraReference();
                GetFuncScriptFromInfra();
                GetFuncScriptFromTenant();
                CompareScripts();

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }


        void GetFuncScriptFromTenant()
        {
            string str = string.Empty;
            StringBuilder hash = new StringBuilder();
            MD5CryptoServiceProvider md5provider = new MD5CryptoServiceProvider();
            string result = string.Empty;
            string vendor = this.EbConnectionFactory.DataDB.Vendor.ToString();
            dictTenant.Clear();
            if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.PGSQL)
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

                EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(str);
                if (dt != null && dt.Rows.Count > 0)
                {
                    for (int i = 0; i < dt.Rows.Count; i++)
                    {
                        result = dt.Rows[i][0].ToString();
                        byte[] bytes = md5provider.ComputeHash(new UTF8Encoding().GetBytes(result));
                        hash.Clear();
                        for (int j = 0; j < bytes.Length; j++)
                        {
                            hash.Append(bytes[j].ToString("x2"));
                        }
                        dictTenant.Add(dt.Rows[i][1].ToString(), new[] { vendor, hash.ToString() });
                    }
                }
            }

        }

        void GetFuncScriptFromInfra()
        {
            //string vendor = this.InfraConnectionFactory.DataDB.Vendor.ToString();
            string vendor = "PGSQL";
            string type = "FUNCTION";
            dictInfra.Clear();
            string str = string.Format(@"
                        SELECT filename, md5 
                        FROM eb_dbmd5 
                        WHERE vendor = '{0}'
                        AND type='{1}'
                        AND eb_del = 'F'", vendor, type);
            EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
            if (dt != null && dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    dictInfra.Add(dt.Rows[i][0].ToString(),new[] {vendor, dt.Rows[i][1].ToString().Trim() });
                }
            }
        }

        void CompareScripts()
        {
            string vendor = "PGSQL";
            if (true)
            {
                foreach (var pair in dictInfra)
                {
                    if (dictTenant.TryGetValue(pair.Key, out string[] value))
                    {
                        if (value[1] != pair.Value[1])
                        {
                            Console.WriteLine(pair.Key + " : change found");
                            UpdateDB(vendor, pair.Key);
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



        void SetFuncMd5InfraReference()
        {
            StringBuilder hash = new StringBuilder();
            MD5CryptoServiceProvider md5provider = new MD5CryptoServiceProvider();
            string result = string.Empty;
            string file_name = string.Empty;
            string type = "FUNCTION";
            string[] fname;
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
                                if (vendor == "PGSQL")
                                {
                                    result = FormatStringPGSQL(result);
                                }

                                byte[] bytes = md5provider.ComputeHash(new UTF8Encoding().GetBytes(result));
                                hash.Clear();
                                for (int j = 0; j < bytes.Length; j++)
                                {
                                    hash.Append(bytes[j].ToString("x2"));
                                }

                                fname = file.Split(".");
                                file_name = fname[2];
                                dictToDB.Add(file_name,new[] { vendor,hash.ToString() });
                                Console.WriteLine(file_name + "Success");

                                using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                                {
                                    con.Open();
                                    string str = string.Format(@"
                                    SELECT * 
                                    FROM eb_dbmd5 
                                    WHERE filename = '{0}' 
                                        AND eb_del = 'F' 
                                        AND vendor = '{1}'", file_name, vendor);
                                    EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
                                    if (dt != null && dt.Rows.Count > 0)
                                    {
                                        string str1 = string.Format(@"
                                            SELECT * 
                                            FROM eb_dbmd5 
                                            WHERE filename = '{0}' 
                                                AND eb_del = 'F' 
                                                AND vendor = '{1}'
                                                AND md5 <> '{2}'", file_name, vendor, hash.ToString());
                                        EbDataTable dt1 = InfraConnectionFactory.DataDB.DoQuery(str1);
                                        if (dt1 != null && dt1.Rows.Count > 0)
                                        {
                                            string str2 = string.Format(@"
                                                    UPDATE eb_dbmd5 
                                                    SET eb_del = 'T'
                                                    WHERE filename = '{0}' 
                                                        AND eb_del = 'F'
                                                        AND vendor = '{1}'", file_name, vendor);
                                            DbCommand cmd1 = InfraConnectionFactory.DataDB.GetNewCommand(con, str2);
                                            cmd1.ExecuteNonQuery();
                                            string str3 = string.Format(@"
                                                    INSERT INTO 
                                                        eb_dbmd5 (filename, vendor, type, md5, eb_del)
                                                    VALUES ('{0}','{1}','{2}','{3}','F')", file_name, vendor, type, hash.ToString());
                                            DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str3);
                                            cmd.ExecuteNonQuery();
                                        }
                                    }
                                    else
                                    {
                                        string str4 = string.Format(@"
                                            INSERT INTO 
                                                eb_dbmd5 (filename, vendor, type, md5, eb_del)
                                            VALUES ('{0}','{1}','{2}','{3}','F')", file_name, vendor, type, hash.ToString());
                                        DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str4);
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }


        void UpdateDB(string vendor,string filename)
        {
            string result = string.Empty;
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
            string Urlstart = string.Format("ExpressBase.Common.sqlscripts.{0}.", vendor.ToLower());
            foreach (string file in func_create)
            {
                if(file.Contains(filename))
                {
                    string path = Urlstart + file;
                    var assembly = typeof(sqlscripts).Assembly;
                    using (Stream stream = assembly.GetManifestResourceStream(path))
                    {
                        if (stream != null)
                        {
                            using (StreamReader reader = new StreamReader(stream))
                                result = reader.ReadToEnd();
                            string fun = GetFuncDef(result);

                        }
                    }
                }
                
            }
        }

        string GetFuncDef(string str)
        {
            string[] sp = str.Split("$BODY$");
            str = sp[2];

            return str;
        }

        string FormatStringPGSQL(string str)
        {
            string[] split = str.Split("$BODY$");
            if (split.Length == 3)
            {

                str = split[0];
                string[] split1 = str.Split("\r\n\r\n");
                str = split1[2];
                str = str.Trim();
                str = str.Replace("(\r\n\t", "(");
                str = str.Replace("\r\n\t", " ");
                str = str.Replace("\r\n", "\n ");
                str = str.Replace(" \n     ", "\n ");
                str = str.Replace("'plpgsql'", "plpgsql");
                str = str.Replace("     ", " ");
                str = str + "\nAS $function$";
                str = str + split[1];
                str = str + "$function$\n";
                str = str.Replace("\r", "");
            }
            else
            {
                str = "";
            }
            return str;
        }
    }
}
    

