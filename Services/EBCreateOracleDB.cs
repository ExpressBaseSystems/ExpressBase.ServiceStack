using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    public class EBCreateOracleDB : EbBaseService
    {

        public EBCreateOracleDB(ITenantDbFactory _dbf) : base(_dbf) { }


        public bool Any(EbCreateOracleDBRequest request)
        {


            bool rtn;
            using (var con1 = TenantDbFactory.DataDB.GetNewConnection())
            {
                con1.Open();

                var con_trans = con1.BeginTransaction();
                //try
                //{
                //......*********USER TABLE******************.......

                //.......create user table sequence...............
                string path = "ExpressBase.Common.SqlScripts.Oracle.DataDb.Alter_Eb_User_SequencesOracle.sql";
                bool b1 = CreateOrAlter_Structure(con1, path);

                //.........create user table........................
                path = "ExpressBase.Common.SqlScripts.Orcale.DataDb.Create_Eb_UserOracle.sql";
                bool b2 = CreateOrAlter_Structure(con1, path);

                //....create trigger for auto increment sequence..........

                path = "ExpressBase.Common.SqlScripts.Oracle.DataDb.Create_Eb_user_TriggerOracle.sql";
                bool b3 = CreateOrAlter_Structure(con1, path);

                //.......create user table Functions...............
                path = "ExpressBase.Common.SqlScripts.Orcale.DataDb.Create_Eb_User_FunctionsOracle.sql";
                bool b4 = CreateOrAlter_Structure(con1, path);

                //...........*************OBJECT TABLE***************...........

                //...........create object table sequences...........
                path = "ExpressBase.Common.SqlScripts.Oracle.ObjectsDb.Alter_Eb_Object_SequencesOracle.sql";
                bool b5 = CreateOrAlter_Structure(con1, path);

                //.......create object tables..............
                path = "ExpressBase.Common.SqlScripts.Oracle.ObjectsDb.Create_Eb_ObjectsOracle.sql";
                bool b6 = CreateOrAlter_Structure(con1, path);

                //....create trigger for auto increment sequence..........
                path = "ExpressBase.Common.SqlScripts.Oracle.ObjectsDb.Create_Eb_Object_TriggerOracle.sql";
                bool b7 = CreateOrAlter_Structure(con1, path);

                //.......create object table functions...........
                path = "ExpressBase.Common.SqlScripts.Oracle.ObjectsDb.Create_Eb_Object_FunctionsOracle.sql";
                bool b8 = CreateOrAlter_Structure(con1, path);

                //.....insert into user tables.........
                bool b9 = InsertIntoTables(request, con1);

                if (b1 & b2 & b3 & b4 & b5 & b6 & b7 & b8 & b9)
                {
                    con_trans.Commit();
                    rtn = true;
                }
                else
                {
                    con_trans.Rollback();
                    rtn = false;
                }
                //con_trans.Commit();
                // b = true;

                //}
                //catch (Exception e)
                //{
                //    con_trans.Rollback();
                //    return false;
                //}
            }
            return rtn;


        }
        public bool CreateOrAlter_Structure(DbConnection con, string path)
        {

            try
            {
                string result;

                var assembly = typeof(ExpressBase.Common.Resource).Assembly;

                //.....................create tbls........
                using (Stream stream = assembly.GetManifestResourceStream(path))
                {
                    using (StreamReader reader = new StreamReader(stream))
                    {
                        result = reader.ReadToEnd();
                    }
                    var cmdtxt1 = TenantDbFactory.DataDB.GetNewCommand(con, result);
                    cmdtxt1.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                return false;
            }
            return true;
        }

        public bool InsertIntoTables(EbCreateOracleDBRequest request, DbConnection con)
        {
            try
            {
                //.......select details from server tbl eb_usres.........
                string sql1 = "SELECT email,pwd,firstname,socialid FROM eb_users WHERE id=@uid";
                DbParameter[] parameter = { this.TenantDbFactory.ObjectsDB.GetNewParameter("@uid", System.Data.DbType.Int32, request.UserId) };
                var rslt = this.TenantDbFactory.ObjectsDB.DoQuery(sql1, parameter);

                //..............insert into client tbl eb_users............ 
                string sql2 = "INSERT INTO eb_users(email,pwd,firstname,socialid) VALUES (@email,@pwd,@firstname,@socialid);";
                var cmdtxt3 = TenantDbFactory.DataDB.GetNewCommand(con, sql2);
                cmdtxt3.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("email", System.Data.DbType.String, rslt.Rows[0][0]));
                cmdtxt3.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("pwd", System.Data.DbType.String, rslt.Rows[0][1]));
                cmdtxt3.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("firstname", System.Data.DbType.String, rslt.Rows[0][2]));
                cmdtxt3.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("socialid", System.Data.DbType.String, rslt.Rows[0][3]));
                cmdtxt3.ExecuteNonQuery();

                //.....select tenant id from eb_users tbl of client....
                string sql3 = "SELECT max(id) FROM eb_users;";
                //var id= TenantDbFactory.DataDB.DoQuery(sql3);
                DbCommand cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql3);
                var id = cmd.ExecuteScalar().ToString();

                //.......add role to tenant as a/c owner
                string sql4 = "";
                foreach (var role in Enum.GetValues(typeof(SystemRoles)))
                {
                    sql4 += string.Format("INSERT INTO eb_role2user(role_id,user_id,createdat) VALUES ({0},{1},now());", (int)role, id);
                }
                var cmdtxt5 = TenantDbFactory.DataDB.GetNewCommand(con, sql4);
                cmdtxt5.ExecuteNonQuery();
            }
            catch (Exception e) { return false; }
            return true;
        }

    }
}
