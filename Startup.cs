using System;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Funq;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Logging;
using ServiceStack.ProtoBuf;
using ServiceStack.Redis;
using System.IdentityModel.Tokens.Jwt;
using System.Collections.Generic;
using ExpressBase.ServiceStack.Auth0;
using System.IO;
using ExpressBase.Common;
using ExpressBase.Data;

namespace ExpressBase.ServiceStack
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
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
        {
            loggerFactory.AddConsole(Configuration.GetSection("Logging"));
            loggerFactory.AddDebug();
            var redisserver = Configuration.GetValue<string>("EbRedisConfig:RedisServer");
            var redispassword = Configuration.GetValue<string>("EbRedisConfig:RedisPassword");
            var redisport = Configuration.GetValue<int>("EbRedisConfig:RedisPort");
            var prikey = Configuration.GetValue<string>("JwtConfig:PrivateKeyXml");
            var pubkey = Configuration.GetValue<string>("JwtConfig:PublicKeyXml");
            EbLiveSettings ELive = new EbLiveSettings(redisserver, redispassword, redisport, prikey, pubkey);
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

            app.UseServiceStack(new AppHost() { EbLiveSettings= ELive });
        }
    }

    public class AppHost : AppHostBase
    {
        public EbLiveSettings EbLiveSettings { get; set; }

        public AppHost() : base("Test Razor", typeof(AppHost).GetAssembly()) { }

        public override void OnAfterConfigChanged()
        {
            base.OnAfterConfigChanged();
        }

        public override void Configure(Container container)
        {
            var co = this.Config;
            LogManager.LogFactory = new ConsoleLogFactory(debugEnabled: true);

            var jwtprovider = new JwtAuthProvider(AppSettings)
            {
                HashAlgorithm = "RS256",
                PrivateKeyXml = EbLiveSettings.PrivateKeyXml,
                PublicKeyXml = EbLiveSettings.PublicKeyXml,
#if (DEBUG)
                RequireSecureConnection = false,
                //EncryptPayload = true,
#endif
                CreatePayloadFilter = (payload, session) => {
                    payload["sub"] = (session as AuthUserSession).UserName;
                    payload["cid"] = (session as AuthUserSession).Company;
                    payload["uid"] = (session as AuthUserSession).UserAuthId;
                    payload["wc"] = (session as CustomUserSession).WhichConsole;
                },
                //PopulateSessionFilter = (session, obj, req) => {
                //    (session as AuthUserSession).Company = obj["cid"];
                //    (session as AuthUserSession).UserAuthId = obj["uid"];
                //    (session as AuthUserSession).UserName = obj["sub"];
                //},

                ExpireTokensIn = TimeSpan.FromSeconds(90),
                ExpireRefreshTokensIn = TimeSpan.FromHours(12),
                PersistSession = true,
                SessionExpiry = TimeSpan.FromHours(12)
            };

            this.Plugins.Add(new CorsFeature(allowedHeaders: "Content-Type, Authorization"));
            this.Plugins.Add(new ProtoBufFormat());

            Plugins.Add(new AuthFeature(() => new CustomUserSession(),
                new IAuthProvider[] {
                    new MyFacebookAuthProvider(AppSettings)
                    {
                        AppId = "151550788692231",
                        AppSecret = "94ec1a04342e5cf7e7a971f2eb7ad7bc",
                        Permissions = new string[] { "email, public_profile" }
                    },
                    new MyCredentialsAuthProvider(AppSettings)
                    {
                        PersistSession = true
                    },
                    jwtprovider,
                }));

            //Also works but it's recommended to handle 404's by registering at end of .NET Core pipeline
            //this.CustomErrorHttpHandlers[HttpStatusCode.NotFound] = new RazorHandler("/notfound");

            Plugins.Add(new RegistrationFeature());

            this.ContentTypes.Register(MimeTypes.ProtoBuf, (reqCtx, res, stream) => ProtoBuf.Serializer.NonGeneric.Serialize(stream, res), ProtoBuf.Serializer.NonGeneric.Deserialize);

            SetConfig(new HostConfig { DebugMode = true });
            SetConfig(new HostConfig { DefaultContentType = MimeTypes.Json });

            var redisConnectionString = string.Format("redis://{0}@{1}:{2}?ssl=true",
               EbLiveSettings.RedisPassword,EbLiveSettings.RedisServer, EbLiveSettings.RedisPort);
            container.Register<IRedisClientsManager>(c => new RedisManagerPool(redisConnectionString));

            container.Register<IUserAuthRepository>(c => new RedisAuthRepository(c.Resolve<IRedisClientsManager>()));

            container.Register<JwtAuthProvider>(jwtprovider);

            string path = Path.Combine(Directory.GetParent(System.IO.Directory.GetCurrentDirectory()).FullName, "EbInfra.conn");
            container.Register<DatabaseFactory>(new DatabaseFactory(EbSerializers.ProtoBuf_DeSerialize<EbInfraDBConf>(EbFile.Bytea_FromFile(path))));

            //Add a request filter to check if the user has a session initialized
            this.GlobalRequestFilters.Add((req, res, requestDto) => 
            {
                if (requestDto.GetType() != typeof(Authenticate) && requestDto.GetType() != typeof(GetAccessToken) && requestDto.GetType() != typeof(InfraRequest))
                {
                    var jwtoken = new JwtSecurityToken((requestDto as IEbSSRequest).Token);
                    if (jwtoken == null)
                        res.ReturnAuthRequired();
                    foreach (var c in jwtoken.Claims)
                    {
                        if (c.Type == "cid" && !string.IsNullOrEmpty(c.Value))
                        {
                            (requestDto as IEbSSRequest).TenantAccountId = c.Value;
                            continue;
                        }
                        if (c.Type == "uid" && !string.IsNullOrEmpty(c.Value))
                        {
                            (requestDto as IEbSSRequest).UserId = Convert.ToInt32(c.Value);
                            continue;
                        }
                    }
                }
            });

            this.GlobalResponseFilters.Add((req, res, responseDto) =>
            {
                if (responseDto.GetResponseDto() != null)
                {
                    if (responseDto.GetResponseDto().GetType() != typeof(MyAuthenticateResponse) && responseDto.GetResponseDto().GetType() != typeof(GetAccessTokenResponse))
                    {
                        // (responseDto.GetResponseDto() as IEbSSResponse).Token = req.Authorization.Replace("Bearer", string.Empty);
                    }
                }
            });
        }
    }
}
