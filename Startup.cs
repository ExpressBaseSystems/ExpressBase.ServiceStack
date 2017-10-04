using ExpressBase.Common.Data;
using ExpressBase.Objects.Objects.MQRelated;
using ExpressBase.Objects.Objects.TenantConnectionsRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.ServiceStack.Auth0;
using Funq;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RestSharp;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.ProtoBuf;
using ServiceStack.RabbitMq;
using ServiceStack.Redis;
using System;
using System.IdentityModel.Tokens.Jwt;

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
            var rabbituser = Configuration.GetValue<string>("EbRabbitMqConfig:RabbitUser");
            var rabbitpassword = Configuration.GetValue<string>("EbRabbitMqConfig:RabbitPassword");
            var rabbithost = Configuration.GetValue<string>("EbRabbitMqConfig:RabbitHost");
            var rabbitport = Configuration.GetValue<int>("EbRabbitMqConfig:RabbitPort");
            var rabbitvhost = Configuration.GetValue<string>("EbRabbitMqConfig:RabbitVHost");
            EbLiveSettings ELive = new EbLiveSettings(redisserver, redispassword, redisport, prikey, pubkey, rabbituser, rabbitpassword, rabbithost, rabbitport, rabbitvhost);
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

            app.UseServiceStack(new AppHost() { EbLiveSettings = ELive });
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

            var jwtprovider = new MyJwtAuthProvider(AppSettings)
            {
                HashAlgorithm = "RS256",
                PrivateKeyXml = EbLiveSettings.PrivateKeyXml,
                PublicKeyXml = EbLiveSettings.PublicKeyXml,
#if (DEBUG)
                RequireSecureConnection = false,
                //EncryptPayload = true,
#endif
                CreatePayloadFilter = (payload, session) =>
                {
                    payload["sub"] = (session as CustomUserSession).UserAuthId;
                    payload["cid"] = (session as CustomUserSession).CId;
                    payload["uid"] = (session as CustomUserSession).Uid.ToString();
                    payload["wc"] = (session as CustomUserSession).WhichConsole;
                },

                ExpireTokensIn = TimeSpan.FromHours(10),
                ExpireRefreshTokensIn = TimeSpan.FromHours(12),
                PersistSession = true,
                SessionExpiry = TimeSpan.FromHours(12)
            };

            this.Plugins.Add(new CorsFeature(allowedHeaders: "Content-Type, Authorization, Access-Control-Allow-Origin, Access-Control-Allow-Credentials"));
            this.Plugins.Add(new ProtoBufFormat());

            Plugins.Add(new AuthFeature(() => new CustomUserSession(),
                new IAuthProvider[] {
                    new MyFacebookAuthProvider(AppSettings)
                    {
                        AppId = "151550788692231",
                        AppSecret = "94ec1a04342e5cf7e7a971f2eb7ad7bc",
                        Permissions = new string[] { "email, public_profile" }
                    },

                    new MyTwitterAuthProvider(AppSettings)
                    {
                        ConsumerKey = "L0ryLVB5hCXy3qHUnyKiYezUk",
                        ConsumerSecret = "61zVZ96nwJb7sadM7v8RMd1Te6jGUHvFcfjaz7vIUdZLXl0cwD",
                        CallbackUrl = "https://localhost:44377/auth/twitter",
                        
                      //  RequestTokenUrl= "https://api.twitter.com/oauth/authenticate",
                        
                    },

                    new MyGithubAuthProvider(AppSettings)
                    {
                    ClientId="07f639367f3e7f066ab9",
                    ClientSecret="7ba2e0662f9dd9d3b7817ebf0adecc00e8ab5b6a",
                    RedirectUrl ="https://localhost:44377/"

                    },

                    new MyCredentialsAuthProvider(AppSettings)
                    {
                        PersistSession = true
                    },
                    jwtprovider,
                }));

            //Also works but it's recommended to handle 404's by registering at end of .NET Core pipeline
            //this.CustomErrorHttpHandlers[HttpStatusCode.NotFound] = new RazorHandler("/notfound");

            Plugins.Add(new EbRegistrationFeature());

            this.ContentTypes.Register(MimeTypes.ProtoBuf, (reqCtx, res, stream) => ProtoBuf.Serializer.NonGeneric.Serialize(stream, res), ProtoBuf.Serializer.NonGeneric.Deserialize);

            SetConfig(new HostConfig { DebugMode = true });
            SetConfig(new HostConfig { DefaultContentType = MimeTypes.Json });

            var redisConnectionString = string.Format("redis://{0}@{1}:{2}?ssl=true",
               EbLiveSettings.RedisPassword, EbLiveSettings.RedisServer, EbLiveSettings.RedisPort);

            container.Register<IRedisClientsManager>(c => new RedisManagerPool(redisConnectionString));

            container.Register<IUserAuthRepository>(c => new EbRedisAuthRepository(c.Resolve<IRedisClientsManager>()));

            container.Register<JwtAuthProvider>(jwtprovider);

            container.Register<ITenantDbFactory>(c => new TenantDbFactory(c)).ReusedWithin(ReuseScope.Request);

            //Message Queue
            //var redisConnectionStringMq = string.Format("redis://{0}@{1}:{2}?ssl=true&db=1",
            //    EbLiveSettings.RedisPassword, EbLiveSettings.RedisServer, EbLiveSettings.RedisPort);

            //var redisFactory = new PooledRedisClientManager(redisConnectionStringMq);
            //var mqHost = new RedisMqServer(redisFactory, retryCount: 2);
            //mqHost.RegisterHandler<EmailRequest>(base.ExecuteMessage);
            //mqHost.RegisterHandler<RefreshSolutionConnectionsRequests>(base.ExecuteMessage);
            //mqHost.RegisterHandler<UploadFileMqRequest>(base.ExecuteMessage, 5);

            //mqHost.Start();

            RabbitMqMessageFactory rabitFactory = new RabbitMqMessageFactory();
            rabitFactory.ConnectionFactory.UserName = EbLiveSettings.RabbitUser;
            rabitFactory.ConnectionFactory.Password = EbLiveSettings.RabbitPassword;
            rabitFactory.ConnectionFactory.HostName = EbLiveSettings.RabbitHost;
            rabitFactory.ConnectionFactory.Port = EbLiveSettings.RabbitPort;
            rabitFactory.ConnectionFactory.VirtualHost = EbLiveSettings.RabbitVHost;

            //rabitFactory.ConnectionFactory.Uri = "amqp://user:2nuGqFcd7uI5@13.84.189.113:5672/MessageQueue";
            var mqServer = new RabbitMqServer(rabitFactory);
            mqServer.RetryCount = 1;
            mqServer.RegisterHandler<EmailServicesMqRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<RefreshSolutionConnectionsRequests>(base.ExecuteMessage);
            mqServer.RegisterHandler<UploadFileMqRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<ImageResizeMqRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<SlackPostMqRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<SlackAuthMqRequest>(base.ExecuteMessage);

            mqServer.Start();

            container.AddScoped<IMessageProducer, RabbitMqProducer>(serviceProvider =>
            {
                return mqServer.CreateMessageProducer() as RabbitMqProducer;
            });

            container.AddScoped<IMessageQueueClient, RabbitMqQueueClient>(serviceProvider =>
            {
                return mqServer.CreateMessageQueueClient() as RabbitMqQueueClient;
            });

            //Add a request filter to check if the user has a session initialized
            this.GlobalRequestFilters.Add((req, res, requestDto) =>
            {
                if (requestDto.GetType() == typeof(Authenticate))
                {
                    RequestContext.Instance.Items.Add("TenantAccountId", (requestDto as Authenticate).Meta["cid"]);
                }

                if (requestDto != null && requestDto.GetType() != typeof(Authenticate) && requestDto.GetType() != typeof(GetAccessToken) && requestDto.GetType() != typeof(EmailServicesRequest) && requestDto.GetType() != typeof(Register))
                {
                    var auth = req.Headers[HttpHeaders.Authorization];
                    if (string.IsNullOrEmpty(auth))
                        res.ReturnAuthRequired();
                    else
                    {
                        var jwtoken = new JwtSecurityToken(auth.Replace("Bearer", string.Empty).Trim());
                        foreach (var c in jwtoken.Claims)
                        {
                            if (c.Type == "cid" && !string.IsNullOrEmpty(c.Value))
                            {
                                RequestContext.Instance.Items.Add("TenantAccountId", c.Value);
                                if (requestDto is IEbSSRequest)
                                    (requestDto as IEbSSRequest).TenantAccountId = c.Value;
                                if (requestDto is EbServiceStackRequest)
                                    (requestDto as EbServiceStackRequest).TenantAccountId = c.Value;
                                continue;
                            }
                            if (c.Type == "uid" && !string.IsNullOrEmpty(c.Value))
                            {
                                RequestContext.Instance.Items.Add("UserId", Convert.ToInt32(c.Value));
                                if (requestDto is IEbSSRequest)
                                    (requestDto as IEbSSRequest).UserId = Convert.ToInt32(c.Value);
                                if (requestDto is EbServiceStackRequest)
                                    (requestDto as EbServiceStackRequest).UserId = Convert.ToInt32(c.Value);
                                continue;
                            }
                            if (c.Type == "wc" && !string.IsNullOrEmpty(c.Value))
                            {
                                RequestContext.Instance.Items.Add("wc", c.Value);
                                if (requestDto is EbServiceStackRequest)
                                    (requestDto as EbServiceStackRequest).WhichConsole = c.Value.ToString();
                                continue;
                            }
                        }
                    }
                }
            });

            this.GlobalResponseFilters.Add((req, res, responseDto) =>
            {
                if (responseDto.GetResponseDto() != null)
                {
                    if (responseDto.GetResponseDto().GetType() == typeof(GetAccessTokenResponse))
                    {
                        res.SetSessionCookie("Token", (res.Dto as GetAccessTokenResponse).AccessToken);


                    }
                }
            });
        }
    }
}
