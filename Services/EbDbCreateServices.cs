using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class EbDbCreateServices : EbBaseService
    {

        public EbDbCreateServices(ITenantDbFactory _dbf) : base(_dbf) { }

        public EbDbCreateResponse Any(EbDbCreateRequest request)
        {

            using (var con = TenantDbFactory.DataDB.GetNewConnection())
            {
                try
                {
                    con.Open();
                    string sql = string.Format("CREATE DATABASE {0};", request.dbName);
                    var cmd = TenantDbFactory.DataDB.GetNewCommand(con, sql);
                    cmd.ExecuteNonQuery();

                    CreateEbTbls(request, con);
                }

                catch (Exception e)
                {
                    if (e.Data["Code"].ToString() == "42P04")
                    {
                        CreateEbTbls(request, con);
                    }
                    else
                    {
                        return new EbDbCreateResponse() { resp = false };
                    }
                }
            }
            return null;
        }

        public void CreateEbTbls(EbDbCreateRequest request, DbConnection con)
        {
            using (var con1 = TenantDbFactory.DataDB.GetNewConnection(request.dbName.ToLower()))
            {
                con1.Open();

                var con1_trans = con1.BeginTransaction();
                try
                {

                    string result;

                    var assembly = typeof(ExpressBase.Common.Resource).Assembly;

                    //.....................create tbls........
                    using (Stream stream = assembly.GetManifestResourceStream("ExpressBase.Common.SqlScripts.PostGreSql.DataDb.postgres_eb_users.sql"))
                    {
                        using (StreamReader reader = new StreamReader(stream))
                        {
                            result = reader.ReadToEnd();
                        }
                        var cmdtxt1 = TenantDbFactory.DataDB.GetNewCommand(con1, result);
                        cmdtxt1.ExecuteNonQuery();
                    }

                    //.........create tbl fun()...........
                    using (Stream stream = assembly.GetManifestResourceStream("ExpressBase.Common.SqlScripts.PostGreSql.DataDb.Functions.sql"))
                    {
                        using (StreamReader reader = new StreamReader(stream))
                        {
                            result = reader.ReadToEnd();
                        }
                        var cmdtxt2 = TenantDbFactory.DataDB.GetNewCommand(con1, result);
                        cmdtxt2.ExecuteNonQuery();
                    }

                    //.......select details from server tbl eb_usres.........
                    string sql1 = "SELECT email,pwd,firstname,socialid from eb_users where id=@uid";
                    DbParameter[] parameter = { this.TenantDbFactory.ObjectsDB.GetNewParameter("@uid", System.Data.DbType.Int32, request.UserId) };
                    var rslt = this.TenantDbFactory.ObjectsDB.DoQuery(sql1, parameter);

                    //..............insert into client tbl eb_users............ 
                    string sql2 = "INSERT INTO eb_users(email,pwd,firstname,socialid) VALUES (@email,@pwd,@firstname,@socialid);";
                    var cmdtxt3 = TenantDbFactory.DataDB.GetNewCommand(con1, sql2);
                    cmdtxt3.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("email", System.Data.DbType.String, rslt.Rows[0][0]));
                    cmdtxt3.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("pwd", System.Data.DbType.String, rslt.Rows[0][1]));
                    cmdtxt3.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("firstname", System.Data.DbType.String, rslt.Rows[0][2]));
                    cmdtxt3.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("socialid", System.Data.DbType.String, rslt.Rows[0][3]));
                    cmdtxt3.ExecuteNonQuery();

                    //.......add role to tenant as a/c owner
                    string sql4 = "";
                    foreach (var role in Enum.GetValues(typeof(SystemRoles)))
                    {
                        sql4 += string.Format("INSERT INTO eb_role2user(role_id,user_id,createdat) VALUES ({0},{1},now());", (int)role, request.UserId);
                    }
                    var cmdtxt5 = TenantDbFactory.DataDB.GetNewCommand(con1, sql4);
                    cmdtxt5.ExecuteNonQuery();

                    con1_trans.Commit();
                }
                catch (Exception e)
                {
                    con1_trans.Rollback();
                }

            }
        }

    }
}
