using ExpressBase.Common;
using ExpressBase.Security;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    [DataContract]
    [Route("/login", "POST")]
    public class Login : IReturn<LoginResponse>
    {
        [DataMember(Order =1)]
        public string UserName { get; set; }

        [DataMember(Order = 2)]
        public string Password { get; set; }
    }

    [DataContract]
    public class LoginResponse
    {
        [DataMember(Order = 1)]
        public User AuthenticatedUser { get; set; }
    }

    [ClientCanSwapTemplates]
    public class LoginService : Service
    {
        public LoginResponse Any(Login request)
        {
            User u = User.GetDetails(request.UserName, request.Password);
            return new LoginResponse
            {
                AuthenticatedUser = u
            };
        }

        public static EbConfiguration ReadTestConfiguration(string path)
        {
            return EbSerializers.ProtoBuf_DeSerialize<EbConfiguration>(EbFile.Bytea_FromFile(path));
        }

        private EbConfiguration LoadTestConfiguration()
        {
            InitDb(@"G:\xyz1.conn");
            return ReadTestConfiguration(@"G:\xyz1.conn");
        }

        private void InitDb(string path)
        {
            EbConfiguration e = new EbConfiguration()
            {
                ClientID = "xyz0007",
                ClientName = "XYZ Enterprises Ltd.",
                LicenseKey = "00288-22558-25558",
            };

            e.DatabaseConfigurations.Add(EbDatabases.EB_OBJECTS, new EbDatabaseConfiguration(EbDatabases.EB_OBJECTS, DatabaseVendors.PGSQL, "eb_objects", "localhost", 5432, "postgres", "infinity", 500));
            e.DatabaseConfigurations.Add(EbDatabases.EB_DATA, new EbDatabaseConfiguration(EbDatabases.EB_DATA, DatabaseVendors.PGSQL, "eb_objects", "localhost", 5432, "postgres", "infinity", 500));
            e.DatabaseConfigurations.Add(EbDatabases.EB_ATTACHMENTS, new EbDatabaseConfiguration(EbDatabases.EB_ATTACHMENTS, DatabaseVendors.PGSQL, "eb_objects", "localhost", 5432, "postgres", "infinity", 500));
            e.DatabaseConfigurations.Add(EbDatabases.EB_LOGS, new EbDatabaseConfiguration(EbDatabases.EB_LOGS, DatabaseVendors.PGSQL, "eb_objects", "localhost", 5432, "postgres", "infinity", 500));

            byte[] bytea = EbSerializers.ProtoBuf_Serialize(e);
            EbFile.Bytea_ToFile(bytea, path);
        }
    }
}
