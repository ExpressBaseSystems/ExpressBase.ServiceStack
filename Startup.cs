using ExpressBase.Common;
using ExpressBase.Data;
using ExpressBase.Objects;
using Funq;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ServiceStack;
using ServiceStack.Data;
using ServiceStack.Host.Handlers;
using ServiceStack.Logging;
using ServiceStack.Mvc;
using ServiceStack.OrmLite;
using ServiceStack.ProtoBuf;
using ServiceStack.Redis;
using ServiceStack.VirtualPath;
using System;
using System.Data;

namespace RazorRockstars.WebHost
{
    public class Startup
    {
        public Startup(IHostingEnvironment env)
        {
            var builder = new Microsoft.Extensions.Configuration.ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true)
                .AddEnvironmentVariables();
            Configuration = builder.Build();
        }

        public IConfigurationRoot Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            // Add framework services.
            services.AddMvc();
            services.AddMemoryCache();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
        {
            loggerFactory.AddConsole(Configuration.GetSection("Logging"));
            loggerFactory.AddDebug();

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                app.UseBrowserLink();
            }
            else
            {
                app.UseExceptionHandler("/Home/Error");
            }

            app.UseStaticFiles();

            app.UseServiceStack(new AppHost());

            app.UseMvc(routes =>
            {
                routes.MapRoute(
                    name: "default",
                    template: "{controller=Home}/{action=Loginuser}/{id?}");
            });

            app.Use(new RazorHandler("/notfound"));

            //var manager = CacheFactory.Build<string>(p => p.WithMicrosoftMemoryCacheHandle());

            //Other examples of using built-in ServiceStack Handlers as middleware
            //app.Use(new StaticFileHandler("wwwroot/img/react-logo.png").Middleware);
            //app.Use(new RequestInfoHandler().Middleware);
        }
    }

    public class AppHost : AppHostBase
    {
        public AppHost() : base("Test Razor", typeof(AppHost).GetAssembly()) { }

        public override void Configure(Container container)
        {
            Plugins.Add(new RazorFormat());
            Plugins.Add(new ProtoBufFormat());
            //Plugins.Add(new RequestLogsFeature
            //{
            //    RequestLogger = new CsvRequestLogger(
            //    files: new FileSystemVirtualPathProvider(this, Config.WebHostPhysicalPath),
            //    requestLogsPattern: "requestlogs/{year}-{month}/{year}-{month}-{day}.csv",
            //    errorLogsPattern: "requestlogs/{year}-{month}/{year}-{month}-{day}-errors.csv",
            //    appendEvery: TimeSpan.FromSeconds(1)
            //),
            //});

            //Also works but it's recommended to handle 404's by registering at end of .NET Core pipeline
            //this.CustomErrorHttpHandlers[HttpStatusCode.NotFound] = new RazorHandler("/notfound");

            this.ContentTypes.Register(MimeTypes.ProtoBuf, (reqCtx, res, stream) => ProtoBuf.Serializer.NonGeneric.Serialize(stream, res), ProtoBuf.Serializer.NonGeneric.Deserialize);

            SetConfig(new HostConfig { DebugMode = true, DefaultContentType = MimeTypes.Json });
            //SetConfig(new HostConfig { DefaultContentType = MimeTypes.Json });
        }
    }
}
