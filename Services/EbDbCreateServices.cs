using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Data.Common;
using System.IO;

namespace ExpressBase.ServiceStack.Services
{
    public class EbDbCreateServices : EbBaseService
    {
        private EbConnectionFactory _dcConnectionFactory = null;

        private EbConnectionFactory DCConnectionFactory
        {
            get
            {
                if(_dcConnectionFactory == null)
                    _dcConnectionFactory = new EbConnectionFactory(EbConnectionsConfigProvider.DataCenterConnections, CoreConstants.EXPRESSBASE);

                return _dcConnectionFactory;
            }
        }

        public EbDbCreateServices(IEbConnectionFactory _idbf) : base(_idbf) { }

        public EbDbCreateResponse Any(EbDbCreateRequest request)
        {
            EbDbCreateResponse resp = new EbDbCreateResponse();

            using (var con = this.DCConnectionFactory.DataDB.GetNewConnection())
            {
                try
                {
                    con.Open();
                    var cmd = this.DCConnectionFactory.DataDB.GetNewCommand(con, string.Format("CREATE DATABASE {0};", request.dbName));
                    cmd.ExecuteNonQuery();

                    resp.resp = DbOperations(request);
                }

                catch (Exception e)
                {
                    if (e.Data["Code"].ToString() == "42P04")
                        resp.resp = DbOperations(request);
                }
            }

            return resp;
        }

        public bool DbOperations(EbDbCreateRequest request)
        {
            bool rtn = false;

            EbConnectionsConfig _solutionConnections = EbConnectionsConfigProvider.DataCenterConnections;

            _solutionConnections.ObjectsDbConnection.DatabaseName = request.dbName.ToLower();
            _solutionConnections.DataDbConnection.DatabaseName = request.dbName.ToLower();

            using (var con = (new EbConnectionFactory(_solutionConnections, request.TenantAccountId)).DataDB.GetNewConnection())
            {
                con.Open();
                var con_trans = con.BeginTransaction();

                //.......create user table sequence...............
                string path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.Alter_Eb_User_Sequences.sql";
                bool b1 = CreateOrAlter_Structure(request, con, path);

                //.........create user table........................
                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.Create_Eb_User.sql";
                bool b2 = CreateOrAlter_Structure(request, con, path);

                //.......create user table index...............
                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.Create_Eb_User_Indexes.sql";
                bool b3 = CreateOrAlter_Structure(request, con, path);

                //.......create user table Functions...............
                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.Create_Eb_User_Functions.sql";
                bool b4 = CreateOrAlter_Structure(request, con, path);

                //...........*************OBJECT TABLE***************...........

                //...........create object table sequences...........
                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.Alter_Eb_Object_Sequences.sql";
                bool b5 = CreateOrAlter_Structure(request, con, path);

                //.......create object tables..............
                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.Create_Eb_Objects.sql";
                bool b6 = CreateOrAlter_Structure(request, con, path);

                //.......create object table index...........
                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.Create_Eb_Object_Indexes.sql";
                bool b7 = CreateOrAlter_Structure(request, con, path);

                //.......create object table functions...........
                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.Create_Eb_Object_Functions.sql";
                bool b8 = CreateOrAlter_Structure(request, con, path);

                //.....insert into user tables.........
                bool b9 = InsertIntoTables(request, con);

                if (b1 & b2 & b3 & b4 & b5 & b6 & b7 & b8 & b9)
                {
                    con_trans.Commit();
                    rtn = true;
                }
                else
                    con_trans.Rollback();
            }

            return rtn;
        }

        public bool CreateOrAlter_Structure(EbDbCreateRequest request, DbConnection con, string path)
        {
            try
            {
                string result = null;

                var assembly = typeof(ExpressBase.Common.Resource).Assembly;

                //.....................create tbls........
                using (Stream stream = assembly.GetManifestResourceStream(path))
                {
                    using (StreamReader reader = new StreamReader(stream))
                        result = reader.ReadToEnd();

                    var cmdtxt1 = EbConnectionFactory.DataDB.GetNewCommand(con, result);
                    cmdtxt1.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                return false;
            }

            return true;
        }

        public bool InsertIntoTables(EbDbCreateRequest request, DbConnection con)
        {
            try
            {
                //.......select details from server tbl eb_usres......... from INFRA
                string sql1 = "SELECT email, pwd, firstname, socialid FROM eb_users WHERE id=@uid";
                DbParameter[] parameter = { this.InfraConnectionFactory.DataDB.GetNewParameter("@uid", EbDbTypes.Int32, request.UserId) };
				var rslt = this.InfraConnectionFactory.DataDB.DoQuery(sql1, parameter);

                //..............insert into client tbl eb_users............ to SOLUTION
                string sql2 = "INSERT INTO eb_users(email, pwd, firstname, socialid) VALUES (@email, @pwd, @firstname, @socialid) RETURNING id;";
                var cmdtxt3 = EbConnectionFactory.DataDB.GetNewCommand(con, sql2);
                cmdtxt3.Parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, rslt.Rows[0][0]));
                cmdtxt3.Parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("pwd", EbDbTypes.String, rslt.Rows[0][1]));
                cmdtxt3.Parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("firstname", EbDbTypes.String, rslt.Rows[0][2]));
                cmdtxt3.Parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("socialid", EbDbTypes.String, rslt.Rows[0][3]));
				var id = Convert.ToInt32(cmdtxt3.ExecuteScalar());

                //.......add role to tenant as a/c owner
                string sql4 = string.Empty;
                foreach (var role in Enum.GetValues(typeof(SystemRoles)))
                {
                    sql4 += string.Format("INSERT INTO eb_role2user(role_id, user_id, createdat) VALUES ({0}, {1}, now());", (int)role, id);
                }

                var cmdtxt5 = EbConnectionFactory.DataDB.GetNewCommand(con, sql4);
                cmdtxt5.ExecuteNonQuery();
            }
            catch (Exception e) { return false; }
            return true;
        }
    }
}
