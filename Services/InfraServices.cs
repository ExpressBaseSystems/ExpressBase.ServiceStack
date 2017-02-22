using ExpressBase.Common;
using ExpressBase.Data;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    [DataContract]
    //[Route("/insert", "POST")]
    public class InfraRequest : IReturn<bool>
    {
        [DataMember(Order = 0)]
        public Dictionary<string, object> Colvalues { get; set; }
    }
    [ClientCanSwapTemplates]
    public class InfraServices : EbBaseService
    {
        public bool Any(InfraRequest request)
        {
            string path = Directory.GetParent(System.IO.Directory.GetCurrentDirectory()).FullName;
            var infraconf = EbSerializers.ProtoBuf_DeSerialize<EbInfraDBConf>(EbFile.Bytea_FromFile(Path.Combine(path, "EbInfra.conn")));
            var df = new DatabaseFactory(infraconf);
            using (var con = df.InfraDB.GetNewConnection())
            {
                con.Open();
                var cmd = df.InfraDB.GetNewCommand(con, "INSERT INTO eb_clients (cid, cname,firstname,lastname,jobtitle,company,phone,employees,zipcode,language,password) VALUES (@cid, @cname, @firstname,@lastname,@jobtitle,@company,@phone,@employees,@zipcode,@language,@password);");
                cmd.Parameters.Add(df.InfraDB.GetNewParameter("cid", System.Data.DbType.String, request.Colvalues["cid"]));
                cmd.Parameters.Add(df.InfraDB.GetNewParameter("cname", System.Data.DbType.String, request.Colvalues["email"]));
                cmd.Parameters.Add(df.InfraDB.GetNewParameter("firstname", System.Data.DbType.String, request.Colvalues["firstname"]));
                cmd.Parameters.Add(df.InfraDB.GetNewParameter("lastname", System.Data.DbType.String, request.Colvalues["lastname"]));
                cmd.Parameters.Add(df.InfraDB.GetNewParameter("jobtitle", System.Data.DbType.String, request.Colvalues["jobtitle"]));
                cmd.Parameters.Add(df.InfraDB.GetNewParameter("company", System.Data.DbType.String, request.Colvalues["company"]));
                cmd.Parameters.Add(df.InfraDB.GetNewParameter("phone", System.Data.DbType.String, request.Colvalues["phone"]));
                cmd.Parameters.Add(df.InfraDB.GetNewParameter("employees", System.Data.DbType.String, request.Colvalues["employees"]));
                cmd.Parameters.Add(df.InfraDB.GetNewParameter("zipcode", System.Data.DbType.String, request.Colvalues["zipcode"]));
                cmd.Parameters.Add(df.InfraDB.GetNewParameter("language", System.Data.DbType.String, request.Colvalues["language"]));
                cmd.Parameters.Add(df.InfraDB.GetNewParameter("password", System.Data.DbType.String, request.Colvalues["password"]));
                //cmd.Parameters.Add(df.InfraDB.GetNewParameter("ctier", System.Data.DbType.Int32, "Unlimited"));
                //cmd.Parameters.Add(df.InfraDB.GetNewParameter("conf", System.Data.DbType.Binary, bytea2));
                cmd.ExecuteNonQuery();
            }
            return true;

        }
    }
}
