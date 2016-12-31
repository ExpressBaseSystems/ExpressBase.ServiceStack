using ExpressBase.Common;
using ExpressBase.Security;
using Microsoft.AspNetCore.Http;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    [DataContract]
    [Route("/register", "POST")]
    public class Register : IReturn<RegisterationResponse>
    {
        [DataMember(Order = 1)]
        public string Email { get; set; }

        [DataMember(Order = 2)]
        public string Password { get; set; }

        [DataMember(Order = 3)]
        public string FirstName { get; set; }

        [DataMember(Order = 4)]
        public string LastName { get; set; }

        [DataMember(Order = 5)]
        public string MiddleName { get; set; }

        [DataMember(Order = 6)]
        public DateTime dob { get; set; }

        [DataMember(Order = 7)]
        public string Phnoprimary { get; set; }

        [DataMember(Order = 8)]
        public string Phnosecondary { get; set; }

        [DataMember(Order = 9)]
        public string Landline { get; set; }

        [DataMember(Order = 10)]
        public string Extension { get; set; }

        [DataMember(Order = 11)]
        public string Locale { get; set; }

        [DataMember(Order = 12)]
        public string Alternateemail { get; set; }

        [DataMember(Order = 13)]
        public byte[] Profileimg { get; set; }
    }

    [DataContract]
    public class RegisterationResponse
    {
        [DataMember(Order = 1)]
        public bool Registereduser { get; set; }
    }

    [ClientCanSwapTemplates]
    public class Registerservice : Service
    {
        public RegisterationResponse Any(Register request)
        {
           bool u= User.Create(request.Email, request.Password,request.FirstName,request.LastName,request.MiddleName,request.dob,request.Phnoprimary,request.Phnosecondary,request.Landline,request.Extension,request.Locale,request.Alternateemail,request.Profileimg);
            return new RegisterationResponse
            {
                Registereduser = u
            };
        }

        private void InitDb(string path)
        {
            EbConfiguration e = new EbConfiguration()
            {
                ClientID = "xyz0007",
                ClientName = "XYZ Enterprises Ltd.",
                LicenseKey = "00288-22558-25558",
            };

            e.DatabaseConfigurations.Add(EbDatabases.EB_OBJECTS, new EbDatabaseConfiguration(EbDatabases.EB_OBJECTS, DatabaseVendors.PGSQL, "AlArz2014", "localhost", 5432, "postgres", "infinity", 500));
            e.DatabaseConfigurations.Add(EbDatabases.EB_DATA, new EbDatabaseConfiguration(EbDatabases.EB_DATA, DatabaseVendors.PGSQL, "AlArz2014", "localhost", 5432, "postgres", "infinity", 500));
            e.DatabaseConfigurations.Add(EbDatabases.EB_ATTACHMENTS, new EbDatabaseConfiguration(EbDatabases.EB_ATTACHMENTS, DatabaseVendors.PGSQL, "AlArz2014", "localhost", 5432, "postgres", "infinity", 500));
            e.DatabaseConfigurations.Add(EbDatabases.EB_LOGS, new EbDatabaseConfiguration(EbDatabases.EB_LOGS, DatabaseVendors.PGSQL, "AlArz2014", "localhost", 5432, "postgres", "infinity", 500));

            byte[] bytea = EbSerializers.ProtoBuf_Serialize(e);
            EbFile.Bytea_ToFile(bytea, path);
        }

        public static EbConfiguration ReadTestConfiguration(string path)
        {
            return EbSerializers.ProtoBuf_DeSerialize<EbConfiguration>(EbFile.Bytea_FromFile(path));
        }

        private EbConfiguration LoadTestConfiguration()
        {
            InitDb(@"D:\xyz1.conn");
            return ReadTestConfiguration(@"D:\xyz1.conn");
        }
    }
}
