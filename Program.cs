using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;

namespace ExpressBase.ServiceStack
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var host = new WebHostBuilder()
                .UseKestrel(options => {
                    options.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(7);
                    options.Limits.RequestHeadersTimeout = TimeSpan.FromMinutes(5);
                })
                .UseContentRoot(Directory.GetCurrentDirectory())
                .UseUrls(urls: "http://*:41600/")
                .UseStartup<Startup>()
                .Build();

            host.Run();
        }
    }
}
