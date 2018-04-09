using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Security;
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
                if (_dcConnectionFactory == null)
                    _dcConnectionFactory = new EbConnectionFactory(EbConnectionsConfigProvider.DataCenterConnections, CoreConstants.EXPRESSBASE);

                return _dcConnectionFactory;
            }
        }

        public EbDbCreateServices(IEbConnectionFactory _idbf) : base(_idbf) { }

        public EbDbCreateResponse Any(EbDbCreateRequest request)
        {

            using (var con = this.DCConnectionFactory.DataDB.GetNewConnection())
            {
                try
                {
                    con.Open();
                    var cmd = this.DCConnectionFactory.DataDB.GetNewCommand(con, string.Format("CREATE DATABASE {0};", request.dbName));
                    cmd.ExecuteNonQuery();

                    return DbOperations(request);
                }

                catch (Exception e)
                {
                    if (e.Data["Code"].ToString() == "42P04")
                        return DbOperations(request);
                }
            }
            return null;

        }

        public EbDbCreateResponse DbOperations(EbDbCreateRequest request)
        {

            EbConnectionsConfig _solutionConnections = EbConnectionsConfigProvider.DataCenterConnections;

            _solutionConnections.ObjectsDbConnection.DatabaseName = request.dbName.ToLower();
            _solutionConnections.DataDbConnection.DatabaseName = request.dbName.ToLower();

            using (var con = request.ischange == "true"? this.EbConnectionFactory.DataDB.GetNewConnection():(new EbConnectionFactory(_solutionConnections, request.TenantAccountId)).DataDB.GetNewConnection())
            {
                con.Open();
                var con_trans = con.BeginTransaction();
                string vendor = this.EbConnectionFactory.DataDB.Vendor.ToString();
                //.............DataDb Tables
                string path = "ExpressBase.Common.SqlScripts.@vendor.eb_extras.sql".Replace("@vendor", vendor);
                bool b1 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.TableCreate.eb_users.sql".Replace("@vendor",vendor);
                bool b2 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.TableCreate.eb_usergroup.sql".Replace("@vendor", vendor);
                bool b3 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.TableCreate.eb_roles.sql".Replace("@vendor", vendor);
                bool b4 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.TableCreate.eb_userstatus.sql".Replace("@vendor", vendor);
                bool b5 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.TableCreate.eb_useranonymous.sql".Replace("@vendor", vendor);
                bool b6 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.TableCreate.eb_role2user.sql".Replace("@vendor", vendor);
                bool b7 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.TableCreate.eb_role2role.sql".Replace("@vendor", vendor);
                bool b8 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.TableCreate.eb_role2permission.sql".Replace("@vendor", vendor);
                bool b9 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.TableCreate.eb_user2usergroup.sql".Replace("@vendor", vendor);
                bool b10 = CreateOrAlter_Structure(con, path, con_trans);

                //.............DataDb Functions

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.FunctionCreate.eb_authenticate_anonymous.sql".Replace("@vendor", vendor);
                bool b11 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.FunctionCreate.eb_authenticate_unified.sql".Replace("@vendor", vendor);
                bool b12 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.FunctionCreate.eb_create_or_update_rbac_manageroles.sql".Replace("@vendor", vendor);
                bool b13 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.FunctionCreate.eb_create_or_update_role.sql".Replace("@vendor", vendor);
                bool b14 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.FunctionCreate.eb_create_or_update_role2role.sql".Replace("@vendor", vendor);
                bool b15 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.FunctionCreate.eb_create_or_update_role2user.sql".Replace("@vendor", vendor);
                bool b16 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.FunctionCreate.eb_createormodifyuserandroles.sql".Replace("@vendor", vendor);
                bool b17 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.FunctionCreate.eb_createormodifyusergroup.sql".Replace("@vendor", vendor);
                bool b18 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.FunctionCreate.eb_getpermissions.sql".Replace("@vendor", vendor);
                bool b19 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.DataDb.FunctionCreate.eb_getroles.sql".Replace("@vendor", vendor);
                bool b20 = CreateOrAlter_Structure(con, path, con_trans);

                //.............ObjectsDb Tables

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.TableCreate.eb_applications.sql".Replace("@vendor", vendor);
                bool b21 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.TableCreate.eb_bots.sql".Replace("@vendor", vendor);
                bool b22 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.TableCreate.eb_files.sql".Replace("@vendor", vendor);
                bool b23 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.TableCreate.eb_objects.sql".Replace("@vendor", vendor);
                bool b24 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.TableCreate.eb_objects_relations.sql".Replace("@vendor", vendor);
                bool b25 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.TableCreate.eb_objects_status.sql".Replace("@vendor", vendor);
                bool b26 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.TableCreate.eb_objects_ver.sql".Replace("@vendor", vendor);
                bool b27 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.TableCreate.eb_objects2application.sql".Replace("@vendor", vendor);
                bool b28 = CreateOrAlter_Structure(con, path, con_trans);

                //.............ObjectsDb Functions

                //path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_botdetails.sql".Replace("@vendor", vendor);
                //bool b29 = CreateOrAlter_Structure(con, path);

                //path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_createbot.sql".Replace("@vendor", vendor);
                //bool b30 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_objects_change_status.sql".Replace("@vendor", vendor);
                bool b31 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_objects_commit.sql".Replace("@vendor", vendor);
                bool b32 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_objects_create_major_version.sql".Replace("@vendor", vendor);
                bool b33 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_objects_create_minor_version.sql".Replace("@vendor", vendor);
                bool b34 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_objects_create_new_object.sql".Replace("@vendor", vendor);
                bool b35 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_objects_create_patch_version.sql".Replace("@vendor", vendor);
                bool b36 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_objects_exploreobject.sql".Replace("@vendor", vendor);
                bool b37 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_objects_getversiontoopen.sql".Replace("@vendor", vendor);
                bool b38 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_objects_save.sql".Replace("@vendor", vendor);
                bool b39 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_objects_update_dashboard.sql".Replace("@vendor", vendor);
                bool b40 = CreateOrAlter_Structure(con, path, con_trans);

                //path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_update_rel.sql".Replace("@vendor", vendor);
              //  bool b43 = CreateOrAlter_Structure(con, path, con_trans);

                path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_get_tagged_object.sql".Replace("@vendor", vendor);
                bool b44 = CreateOrAlter_Structure(con, path, con_trans);

               

                //.....insert into user tables.........
                bool b41 = InsertIntoTables(request, con, con_trans);

                var b42 = request.ischange == "true" ? null : CreateUsers4DataBase(con, request);

                if (b1 & b2 & b3 & b4 & b5 & b6 & b7 & b8 & b9 & b10 & b11 & b12 & b13 & b14 & b15 & b16 & b17 & b18 & b19 &
                    b20 & b21 & b22 & b23 & b24 & b25 & b26 & b27 & b28 & b31 & b32 & b33 & b34 & b35 & b36 & b37 & b38 & b39 & b40 & b41 & b44)
                {
                    con_trans.Commit();
                    var success = request.ischange == "true" ? new EbDbCreateResponse() { resp = true } : b42;
                    return success;
                }
                else
                    con_trans.Rollback();
            }

            return null;
        }

        public EbDbCreateResponse CreateUsers4DataBase(DbConnection con, EbDbCreateRequest request)
        {
            try
            {
               
                string usersql = "SELECT * FROM eb_assignprivileges('@unameadmin','@unameROUser','@unameRWUser');".Replace("@unameadmin", request.dbName + "_admin").Replace("@unameROUser", request.dbName + "_ro").Replace("@unameRWUser", request.dbName + "_rw");

                var dt = this.EbConnectionFactory.DataDB.DoQuery(usersql);

                
                string sql = @"GRANT ALL PRIVILEGES ON DATABASE ""@dbname"" TO @unameadmin;
                               GRANT USAGE ON SCHEMA public TO @unameadmin;
                               GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO @unameadmin;
                               GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO @unameadmin;
                               GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO @unameadmin;
                               GRANT CONNECT ON DATABASE ""@dbname"" TO @unameROUser;
                               GRANT USAGE ON SCHEMA public TO @unameROUser;
                               GRANT SELECT ON ALL TABLES IN SCHEMA public TO @unameROUser;
                               GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO @unameROUser;
                               GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO @unameROUser;
                               GRANT CONNECT ON DATABASE ""@dbname"" TO @unameRWUser;
                               GRANT USAGE ON SCHEMA public TO @unameRWUser;
                               GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA public TO @unameRWUser;
                               GRANT SELECT,UPDATE ON ALL SEQUENCES IN SCHEMA public TO @unameRWUser;
                               GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO @unameRWUser;"
                                    .Replace("@unameadmin", request.dbName + "_admin").Replace("@unameROUser", request.dbName + "_ro")
                                    .Replace("@unameRWUser", request.dbName + "_rw").Replace("@dbname", request.dbName);

                              var  grnt = this.EbConnectionFactory.DataDB.DoNonQuery(sql);
                

                return new EbDbCreateResponse
                {
                    resp = true,
                    AdminUserName = request.dbName + "_admin",
                    AdminPassword = dt.Rows[0][0].ToString(),
                    ReadOnlyUserName = request.dbName + "_ro",
                    ReadOnlyPassword = dt.Rows[0][1].ToString(),
                    ReadWriteUserName = request.dbName + "_rw",
                    ReadWritePassword = dt.Rows[0][2].ToString(),
                    dbname = request.dbName
                };
            }
            catch (Exception e)
            {
                return null;
            }
        }

        public bool CreateOrAlter_Structure(DbConnection con, string path, DbTransaction con_trans)
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

                    var cmdtxt1 = EbConnectionFactory.DataDB.GetNewCommand(con, result, con_trans);
                    cmdtxt1.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                return false;
            }

            return true;
        }

        public bool InsertIntoTables(EbDbCreateRequest request, DbConnection con,DbTransaction con_trans)
        {
            try
            {
                //.......select details from server tbl eb_usres......... from INFRA
                string sql1 = "SELECT email, pwd, firstname, socialid FROM eb_users WHERE id=:uid";
                DbParameter[] parameter = { this.InfraConnectionFactory.DataDB.GetNewParameter("uid", EbDbTypes.Int32, request.UserId) };
                var rslt = this.InfraConnectionFactory.DataDB.DoQuery(sql1, parameter);

                //..............insert into client tbl eb_users............ to SOLUTION
                string sql2 = "INSERT INTO eb_users(email, pwd, fullname, socialid) VALUES (:email, :pwd, :firstname, :socialid) RETURNING id;";
                var cmdtxt3 = EbConnectionFactory.DataDB.GetNewCommand(con, sql2, con_trans);
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

                var cmdtxt5 = EbConnectionFactory.DataDB.GetNewCommand(con, sql4, con_trans);
                cmdtxt5.ExecuteNonQuery();
            }
            catch (Exception e) { return false; }
            return true;
        }
    }
}
