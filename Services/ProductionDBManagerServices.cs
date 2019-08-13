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
        //IDictionary<string[], string[]> dictToDB = new Dictionary<string[], string[]>();

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

                SetFuncMd5InfraReference();
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
                        result = FormatDBStringPGSQL(result);
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
                        SELECT filename, md5, filepath
                        FROM eb_dbmd5 
                        WHERE vendor = '{0}'
                        AND type='{1}'
                        AND eb_del = 'F'", vendor, type);
            EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
            if (dt != null && dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    dictInfra.Add(dt.Rows[i][0].ToString(), new[] { vendor, dt.Rows[i][2].ToString(), dt.Rows[i][1].ToString().Trim() });
                }
            }
        }

        void CompareScripts()
        {
            if (true)
            {
                foreach (var pair in dictInfra)
                {
                    if (dictTenant.TryGetValue(pair.Key, out string[] value))
                    {
                        if (value[1] != pair.Value[1])
                        {
                            Console.WriteLine(pair.Key + " : " + pair.Value[1] + " : " + value[1] + " : change found");
                            //UpdateDB(pair.Value[0], pair.Key, pair.Value[2]);
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
                                    result = FormatFileStringPGSQL(result);
                                }

                                byte[] bytes = md5provider.ComputeHash(new UTF8Encoding().GetBytes(result));
                                hash.Clear();
                                for (int j = 0; j < bytes.Length; j++)
                                {
                                    hash.Append(bytes[j].ToString("x2"));
                                }

                                fname = file.Split(".");
                                file_name = fname[2];
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
                                                        eb_dbmd5 (filename, vendor, type, md5, filepath, eb_del)
                                                    VALUES ('{0}','{1}','{2}','{3}','{4}','F')", file_name, vendor, type, hash.ToString(), file);
                                            DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str3);
                                            cmd.ExecuteNonQuery();
                                        }
                                    }
                                    else
                                    {
                                        string str4 = string.Format(@"
                                            INSERT INTO 
                                                eb_dbmd5 (filename, vendor, type, md5, filepath, eb_del)
                                            VALUES ('{0}','{1}','{2}','{3}','{4}','F')", file_name, vendor, type, hash.ToString(), file);
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


        void UpdateDB(string vendor, string filename, string filepath)
        {
            string result = string.Empty;

            string Urlstart = string.Format("ExpressBase.Common.sqlscripts.{0}.", vendor.ToLower());
            string path = Urlstart + filepath;
            var assembly = typeof(sqlscripts).Assembly;
            using (Stream stream = assembly.GetManifestResourceStream(path))
            {
                if (stream != null)
                {
                    using (StreamReader reader = new StreamReader(stream))
                        result = reader.ReadToEnd();
                    //string fun = GetFuncDef(result);
                    using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                    {
                        con.Open();
                        //string str = @"
                        //            DROP FUNCTION "+ fun ;
                        //DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str);
                        //cmd.ExecuteNonQuery();

                        DbCommand cmd1 = InfraConnectionFactory.DataDB.GetNewCommand(con, result);
                        cmd1.ExecuteNonQuery();
                    }
                }
            }

        }

        string GetFuncDef(string str)
        {
            string[] sp = str.Split("$BODY$");
            str = sp[2];
            string[] sp1 = str.Split(".");
            str = sp1[1];
            string[] sp2 = str.Split(")");
            str = sp2[0] + ")";
            return str;
        }

        string FormatDBStringPGSQL(string str)
        {
            str = str.Replace(" ", "").Replace("\r", "").Replace("\t", "").Replace("\n", "");
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
                str = str.Replace(" ", "").Replace("\r", "").Replace("\t", "").Replace("\n", "").Replace("'plpgsql'", "plpgsqlAS$function$");
            }
            else if(split.Length == 0)
            {
                str = "";
            }

            //if(split.Length == 3)
            //{
            //    string[] split1 =split[0].Split("\r\n\r\n");
            //    str = split1[2].Replace("(\r\n\t", "(").Replace("\r\n\t", " ").Replace(")\r\n   ", ")\n") + split[1];
            //    str = str.Replace(" \r\n    LANGUAGE 'plpgsql'\r\n\r\nDECLARE", "\n LANGUAGE plpgsql\nAS $function$\r\n\r\nDECLARE").Replace("\r\n\r\nEND;\r\n\r\n", "\r\n\r\nEND;\r\n\r\n$function$\n");
            //}

            //if (split.Length == 3)
            //{ 
            //    string[] split1 = str.Split("\r\n\r\n");
            //    if(split1.Length == 3)
            //    {
            //        str = split1[2];
            //        string defenition = split[0].Trim().Replace("\r\n\r\n    COST 100\r\n    VOLATILE\r\n    ROWS 1000\r", "").Replace("(\r\n\t", "(").Replace("\r\n\t", " ").Replace("\r\n", "\n ").Replace(" \n     ", "\n ").Replace("'plpgsql'", "plpgsql").Replace("     ", " ");
            //        string body = split[1];

            //        str = (defenition + " $function$" + body + "$function$\n");
            //    }
            //    else
            //    {
            //        string defenition = split[0].Trim().Replace("(\r\n\t", "(").Replace("\r\n\t", " ").Replace("\r\n", "\n ").Replace(" \n     ", "\n ").Replace("'plpgsql'", "plpgsql").Replace("     ", " ").Replace("\n COST 100\n VOLATILE\n ROWS 1000\n ", "");
            //        string body = split[1];

            //        str = (defenition + " $function$" + body + "$function$\n");
            //    }
            //}
            //else if(split.Length == 1)
            //{
            //    string[] split2 = str.Split("$function$");
            //    str = split2[0];
            //    string[] split3 = str.Split("\n \n ");
            //    str = split3[2];
            //    str = str + split2[1];
            //    string defenition = split[0].Trim().Replace("(\r\n\t", "(").Replace("\r\n\t", " ").Replace("\r\n", "\n ").Replace(" \n     ", "\n ").Replace("'plpgsql'", "plpgsql").Replace("     ", " ").Replace("\n COST 100\n VOLATILE\n ROWS 1000\n ", "");
            //    string body = split[1];

            //    str = (defenition + " $function$" + body + "$function$\n");
            //}
            //else
            //{
            //    str = "";
            //}
            return str;
        }
    }
}


