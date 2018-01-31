using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Data;

namespace ExpressBase.ServiceStack.MQServices
{
    [Restrict(InternalOnly = true)]
    public class RefreshSolutionConnections : EbBaseService
    {
        public bool Post(RefreshSolutionConnectionsMqRequest req)
        {
            using (var con = new EbConnectionFactory(CoreConstants.EXPRESSBASE, this.Redis).DataDB.GetNewConnection() as Npgsql.NpgsqlConnection)
            {
                try
                {
                    con.Open();
                    string sql = @"SELECT con_type, con_obj FROM eb_connections WHERE solution_id = @solution_id AND eb_del = false";
                    DataTable dt = new DataTable();
                    var ada = new Npgsql.NpgsqlDataAdapter(sql, con);
                    ada.SelectCommand.Parameters.Add(new Npgsql.NpgsqlParameter("solution_id", NpgsqlTypes.NpgsqlDbType.Text) { Value = req.TenantAccountId });
                    ada.Fill(dt);

                    EbConnectionsConfig cons = new EbConnectionsConfig();
                    foreach (DataRow dr in dt.Rows)
                    {
                        if (dr["con_type"].ToString() == EbConnectionTypes.EbDATA.ToString())
                            cons.DataDbConnection = EbSerializers.Json_Deserialize<EbDataDbConnection>(dr["con_obj"].ToString());
                        else if (dr["con_type"].ToString() == EbConnectionTypes.EbDATA_RO.ToString())
                            cons.DataDbConnection = EbSerializers.Json_Deserialize<EbDataDbConnection>(dr["con_obj"].ToString());
                        else if (dr["con_type"].ToString() == EbConnectionTypes.EbOBJECTS.ToString())
                            cons.ObjectsDbConnection = EbSerializers.Json_Deserialize<EbObjectsDbConnection>(dr["con_obj"].ToString());
                        else if (dr["con_type"].ToString() == EbConnectionTypes.EbFILES.ToString())
                            cons.FilesDbConnection = EbSerializers.Json_Deserialize<EbFilesDbConnection>(dr["con_obj"].ToString());
                        else if (dr["con_type"].ToString() == EbConnectionTypes.EbLOGS.ToString())
                            cons.LogsDbConnection = EbSerializers.Json_Deserialize<EbLogsDbConnection>(dr["con_obj"].ToString());
                        else if (dr["con_type"].ToString() == EbConnectionTypes.SMTP.ToString())
                            cons.SMTPConnection = EbSerializers.Json_Deserialize<SMTPConnection>(dr["con_obj"].ToString());
                        else if (dr["con_type"].ToString() == EbConnectionTypes.SMS.ToString())
                            cons.SMSConnection = EbSerializers.Json_Deserialize<SMSConnection>(dr["con_obj"].ToString());
                        // ... More to come
                    }

                    Redis.Set<EbConnectionsConfig>(string.Format(CoreConstants.SOLUTION_CONNECTION_REDIS_KEY, req.TenantAccountId), cons);

                    return true;
                }
                catch (Exception e)
                {
                    return false;
                }
            }
        }
    }
}
