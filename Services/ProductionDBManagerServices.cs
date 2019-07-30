using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
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
                    EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery("SELECT solution_id FROM eb_solutions");
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

        IDictionary<string, string> dict = new Dictionary<string, string>();

        public void Post(FunctionCheckRequest request)
        {
            //EbConnectionFactory _ebConnectionFactory;
            //foreach (string name in SolNames)
            //    _ebConnectionFactory = new EbConnectionFactory(name, Redis);
            GetFuncScriptFromInfra();
        }


        void GetFuncScriptFromTenant()
        {
            string str = string.Empty;
            if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.PGSQL)
            {
                str = string.Format(@"
                        SELECT MD5(pg_get_functiondef(oid)::text), proname 
                        FROM pg_proc 
                        WHERE proname 
                        IN 
                            (SELECT routine_name 
                            FROM information_schema.routines 
                            WHERE routine_type = 'FUNCTION' 
                                AND specific_schema = 'public' 
                                AND routine_name like 'eb_%')");
            }
            EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(str);
            if (dt != null && dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    dict.Add(dt.Rows[i][1].ToString(), dt.Rows[i][0].ToString());
                }
            }

        }

        void GetFuncScriptFromInfra()
        {
            StringBuilder hash = new StringBuilder();
            MD5CryptoServiceProvider md5provider = new MD5CryptoServiceProvider();
            string result = string.Empty;
            string file_name = string.Empty;
            string vendor = this.EbConnectionFactory.DataDB.Vendor.ToString();
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
                        byte[] bytes = md5provider.ComputeHash(new UTF8Encoding().GetBytes(result));
                        hash.Clear();
                        for (int i = 0; i < bytes.Length; i++)
                        {
                            hash.Append(bytes[i].ToString("x2"));
                        }
                        fname = file.Split(".");
                        file_name = fname[2];
                        using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                        {
                            con.Open();
                            string str = string.Format(@"
                                    INSERT INTO 
                                        eb_dbmd5 (filename, vendor, type, md5)
                                    VALUES ('{0}','{1}','{2}','{3}')", file_name, vendor, type, hash.ToString());

                            DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str);
                            cmd.ExecuteNonQuery();
                        }
                    }
                }

            }
        }
    }
}

