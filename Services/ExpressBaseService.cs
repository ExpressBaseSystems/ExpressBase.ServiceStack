using System.Collections.Generic;
using ServiceStack;
using ServiceStack.DataAnnotations;
using ServiceStack.OrmLite;
using ServiceStack.Text;
using System.Runtime.Serialization;

namespace RazorRockstars
{
    [Route("/rockstars")]
    [Route("/rockstars/{Id}")]
    [Route("/rockstars/aged/{Age}")]
    public class SearchRockstars : IReturn<RockstarsResponse>
    {
        public int? Age { get; set; }
        public int Id { get; set; }
    }

    [Route("/users")]
    [Route("/users/{Id}")]
    public class SearchUsers : IReturn<UsersResponse>
    {
        public int Id { get; set; }
    }

    [Route("/rockstars/delete/{Id}")]
    public class DeleteRockstar
    {
        public int Id { get; set; }
    }

    [Route("/reset")]
    public class ResetRockstars { }

    [DataContract]
    [Csv(CsvBehavior.FirstEnumerable)]
    public class RockstarsResponse
    {
        [DataMember(Order = 1)]
        public int Total { get; set; }

        [DataMember(Order = 2)]
        public int? Aged { get; set; }

        [DataMember(Order = 3)]
        public List<Rockstar> Results { get; set; }
    }

    [DataContract]
    [Csv(CsvBehavior.FirstEnumerable)]
    public class UsersResponse
    {
        [DataMember(Order = 1)]
        public int Id { get; set; }

        //[DataMember(Order = 2)]
        //public EbDataTable Text { get; set; }
    }

    //Poco Data Model for OrmLite + SeedData 
    [DataContract]
    [Route("/rockstars", "POST")]
    public class Rockstar
    {
        [DataMember(Order =1)]
        [AutoIncrement]
        public int Id { get; set; }

        [DataMember(Order = 2)]
        public string FirstName { get; set; }

        [DataMember(Order = 3)]
        public string LastName { get; set; }

        [DataMember(Order = 4)]
        public int? Age { get; set; }

        [DataMember(Order = 5)]
        public bool Alive { get; set; }

        [DataMember(Order = 6)]
        public string Url { get; set; }

        public Rockstar() { }
        public Rockstar(int id, string firstName, string lastName, int age, bool alive)
        {
            Id = id;
            FirstName = firstName;
            LastName = lastName;
            Age = age;
            Alive = alive;
            Url = "/stars/{0}/{1}/".Fmt(Alive ? "alive" : "dead", LastName.ToLower());
        }
    }

    [ClientCanSwapTemplates]
    [DefaultView("Rockstars")]
    public class RockstarsService : Service
    {
        public static Rockstar[] SeedData = new[] {
            new Rockstar(1, "Jimi", "Hendrix", 27, false),
            new Rockstar(2, "Janis", "Joplin", 27, false),
            new Rockstar(4, "Kurt", "Cobain", 27, false),
            new Rockstar(5, "Elvis", "Presley", 42, false),
            new Rockstar(6, "Michael", "Jackson", 50, false),
            new Rockstar(7, "Eddie", "Vedder", 47, true),
            new Rockstar(8, "Dave", "Grohl", 43, true),
            new Rockstar(9, "Courtney", "Love", 48, true),
            new Rockstar(10, "Bruce", "Springsteen", 62, true),
        };

        public object Get(SearchRockstars request)
        {
            return new RockstarsResponse
            {
                Aged = request.Age,
                Total = Db.Scalar<int>("select count(*) from Rockstar"),
                Results = request.Id != default(int)
                    ? Db.Select<Rockstar>(q => q.Id == request.Id)
                    : request.Age.HasValue
                        ? Db.Select<Rockstar>(q => q.Age == request.Age.Value)
                        : Db.Select<Rockstar>()
            };
        }

        public object Any(DeleteRockstar request)
        {
            Db.DeleteById<Rockstar>(request.Id);
            return Get(new SearchRockstars());
        }

        public object Post(Rockstar request)
        {
            Db.Insert(request);
            return Get(new SearchRockstars());
        }

        public object Any(ResetRockstars request)
        {
            Db.DropAndCreateTable<Rockstar>();
            Db.InsertAll(SeedData);
            return Get(new SearchRockstars());
        }
    }

    //[ClientCanSwapTemplates]
    //[DefaultView("Users")]
    //public class UsersService : Service
    //{
    //    public object Get(SearchUsers request)
    //    {
    //        var e = LoadTestConfiguration();
    //        DatabaseFactory df = new DatabaseFactory(e);
    //        var dt = df.ObjectsDatabase.DoQuery("SELECT * FROM eb_users");
    //        return new UsersResponse()
    //        {
    //            Text = dt
    //            //Text = JsonSerializer.SerializeToString(dt)
    //        };
    //    }

    //    private void InitDb(string path)
    //    {
    //        EbConfiguration e = new EbConfiguration()
    //        {
    //            ClientID = "xyz0007",
    //            ClientName = "XYZ Enterprises Ltd.",
    //            LicenseKey = "00288-22558-25558",
    //        };

    //        e.DatabaseConfigurations.Add(EbDatabases.EB_OBJECTS, new EbDatabaseConfiguration(EbDatabases.EB_OBJECTS, DatabaseVendors.PGSQL, "eb_objects", "localhost", 5432, "postgres", "infinity", 500));
    //        e.DatabaseConfigurations.Add(EbDatabases.EB_DATA, new EbDatabaseConfiguration(EbDatabases.EB_DATA, DatabaseVendors.PGSQL, "eb_objects", "localhost", 5432, "postgres", "infinity", 500));
    //        e.DatabaseConfigurations.Add(EbDatabases.EB_ATTACHMENTS, new EbDatabaseConfiguration(EbDatabases.EB_ATTACHMENTS, DatabaseVendors.PGSQL, "eb_objects", "localhost", 5432, "postgres", "infinity", 500));
    //        e.DatabaseConfigurations.Add(EbDatabases.EB_LOGS, new EbDatabaseConfiguration(EbDatabases.EB_LOGS, DatabaseVendors.PGSQL, "eb_objects", "localhost", 5432, "postgres", "infinity", 500));

    //        byte[] bytea = EbSerializers.ProtoBuf_Serialize(e);
    //        EbFile.Bytea_ToFile(bytea, path);
    //    }

    //    public static EbConfiguration ReadTestConfiguration(string path)
    //    {
    //        return EbSerializers.ProtoBuf_DeSerialize<EbConfiguration>(EbFile.Bytea_FromFile(path));
    //    }

    //    private EbConfiguration LoadTestConfiguration()
    //    {
    //        InitDb(@"D:\xyz1.conn");
    //        return ReadTestConfiguration(@"D:\xyz1.conn");
    //    }
    //}
}
