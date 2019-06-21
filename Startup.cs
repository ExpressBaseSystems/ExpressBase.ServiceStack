using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.EbServiceStack.ReqNRes;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.ServiceStack.Auth;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.ServiceStack.Auth0;
using Funq;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Caching;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.ProtoBuf;
using ServiceStack.RabbitMq;
using ServiceStack.Redis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Reflection;

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
            services.AddDataProtection(opts =>
             {
                 opts.ApplicationDiscriminator = "expressbase.servicestack";
             });
            // Add framework services.
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
        }
    }

    public class AppHost : AppHostBase
    {
        //public EbLiveSettings EbLiveSettings { get; set; }

        private PooledRedisClientManager RedisBusPool { get; set; }

        public AppHost() : base("EXPRESSbase Services", typeof(AppHost).Assembly) { }

        public override void OnAfterConfigChanged()
        {
            base.OnAfterConfigChanged();
        }

        public override void Configure(Container container)
        {
            var co = this.Config;

            LogManager.LogFactory = new ConsoleLogFactory(debugEnabled: true);

            MyJwtAuthProvider jwtprovider = new MyJwtAuthProvider
            {
                HashAlgorithm = "RS256",
                PrivateKeyXml = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_JWT_PRIVATE_KEY_XML),
                PublicKeyXml = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_JWT_PUBLIC_KEY_XML),
#if (DEBUG)
                RequireSecureConnection = false,
                //EncryptPayload = true,
#endif
                ExpireTokensIn = TimeSpan.FromSeconds(90),
                ExpireRefreshTokensIn = TimeSpan.FromHours(24),
                PersistSession = true,
                SessionExpiry = TimeSpan.FromHours(12),

                CreatePayloadFilter = (payload, session) =>
                {
                    payload[TokenConstants.SUB] = (session as CustomUserSession).UserAuthId;
                    payload[TokenConstants.CID] = (session as CustomUserSession).CId;
                    payload[TokenConstants.UID] = (session as CustomUserSession).Uid.ToString();
                    payload[TokenConstants.WC] = (session as CustomUserSession).WhichConsole;
                },

                PopulateSessionFilter = (session, token, req) =>
                {
                    var csession = session as CustomUserSession;
                    csession.UserAuthId = token[TokenConstants.SUB];
                    csession.CId = token[TokenConstants.CID];
                    csession.Uid = Convert.ToInt32(token[TokenConstants.UID]);
                    csession.WhichConsole = token[TokenConstants.WC];
                }
            };

            EbApiAuthProvider apiprovider = new EbApiAuthProvider(AppSettings)
            {
#if (DEBUG)
                RequireSecureConnection = false,
                //EncryptPayload = true,
#endif
            };

            this.Plugins.Add(new CorsFeature(allowedHeaders: "Content-Type, Authorization, Access-Control-Allow-Origin, Access-Control-Allow-Credentials"));

            this.Plugins.Add(new ProtoBufFormat());
            this.Plugins.Add(new SessionFeature());

            this.Plugins.Add(new AuthFeature(() =>
                new CustomUserSession(),
                new IAuthProvider[]
                {
                    new MyCredentialsAuthProvider(AppSettings) { PersistSession = true },
                    jwtprovider
                    //apiprovider,


                    //new MyFacebookAuthProvider(AppSettings)
                    //{
                    //    AppId = "683590648713089",//"151550788692231", 
                    //    AppSecret = "61e674e06ae1b499a7b64b89454aa416",//"94ec1a04342e5cf7e7a971f2eb7ad7bc",
                    //    Permissions = new string[] { "email, public_profile" }
                    //},

                    //new MyTwitterAuthProvider(AppSettings)
                    //{
                    //    ConsumerKey = "6G9gaYo7DMx1OHYRAcpmkPfvu",
                    //    ConsumerSecret = "Jx8uUIPeo5D0agjUnqkKHGQ4o6zTrwze9EcLtjDlOgLnuBaf9x",
                    //   // CallbackUrl = "http://localhost:8000/auth/twitter",
                    //   // RequestTokenUrl= "https://api.twitter.com/oauth/authenticate",
                    //},

                    //new MyGithubAuthProvider(AppSettings)
                    //{
                    //    ClientId = "4504eefeb8f027c810dd",
                    //    ClientSecret = "d9c1c956a9fddd089798e0031851e93a8d0e5cc6",
                    //    RedirectUrl = "http://localhost:8000/"
                    //}
                }));

            this.ContentTypes.Register(MimeTypes.ProtoBuf, (reqCtx, res, stream) => ProtoBuf.Serializer.NonGeneric.Serialize(stream, res), ProtoBuf.Serializer.NonGeneric.Deserialize);

            SetConfig(new HostConfig { DebugMode = true });
            SetConfig(new HostConfig { DefaultContentType = MimeTypes.Json });

            var redisConnectionString = string.Format("redis://{0}@{1}:{2}",
               Environment.GetEnvironmentVariable(EnvironmentConstants.EB_REDIS_PASSWORD),
               Environment.GetEnvironmentVariable(EnvironmentConstants.EB_REDIS_SERVER),
               Environment.GetEnvironmentVariable(EnvironmentConstants.EB_REDIS_PORT));

            container.Register<IRedisClientsManager>(c => new RedisManagerPool(redisConnectionString));

            container.Register<IAuthRepository>(c => new MyRedisAuthRepository(c.Resolve<IRedisClientsManager>()));
            container.Register<IManageApiKeys>(c => new EbApiRedisAuthRepository(c.Resolve<IRedisClientsManager>()));

            container.Register<ICacheClient>(c => new RedisClientManagerCacheClient(c.Resolve<IRedisClientsManager>()));
            container.Register<JwtAuthProvider>(jwtprovider);
            container.Register<IEbConnectionFactory>(c => new EbConnectionFactory(c)).ReusedWithin(ReuseScope.Request);
            container.Register<IEbServerEventClient>(c => new EbServerEventClient()).ReusedWithin(ReuseScope.Request);
            container.Register<IEbMqClient>(c => new EbMqClient()).ReusedWithin(ReuseScope.Request);
            container.Register<IEbStaticFileClient>(c => new EbStaticFileClient()).ReusedWithin(ReuseScope.Request);

            //Setting Assembly version in Redis
            RedisClient client = (container.Resolve<IRedisClientsManager>() as RedisManagerPool).GetClient() as RedisClient;
            AssemblyName assembly = Assembly.GetExecutingAssembly().GetName();
            String version = assembly.Name.ToString() + " - " + assembly.Version.ToString();
            client.Set("ServiceStackAssembly", version);

            RabbitMqMessageFactory rabitFactory = new RabbitMqMessageFactory();
            rabitFactory.ConnectionFactory.UserName = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_USER);
            rabitFactory.ConnectionFactory.Password = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_PASSWORD);
            rabitFactory.ConnectionFactory.HostName = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_HOST);
            rabitFactory.ConnectionFactory.Port = Convert.ToInt32(Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_PORT));
            rabitFactory.ConnectionFactory.VirtualHost = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_VHOST);
            var mqServer = new RabbitMqServer(rabitFactory);

            mqServer.RetryCount = 1;

            mqServer.RegisterHandler<EmailServicesRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<EmailAttachmenRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<ExportApplicationRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<ImportApplicationRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<SMSCreateRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<ReportInternalRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<AddSchedulesToSolutionRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<ExportToExcelServiceRequest>(base.ExecuteMessage);

            mqServer.Start();

            container.AddScoped<IMessageProducer, RabbitMqProducer>(serviceProvider =>
            {
                return mqServer.CreateMessageProducer() as RabbitMqProducer;
            });

            container.AddScoped<IMessageQueueClient, RabbitMqQueueClient>(serviceProvider =>
            {
                return mqServer.CreateMessageQueueClient() as RabbitMqQueueClient;
            });

            this.GlobalRequestFilters.Add((req, res, requestDto) =>
            {
                ILog log = LogManager.GetLogger(GetType());

                log.Info("In GlobalRequestFilters");
                try
                {
                    if (requestDto.GetType() == typeof(Authenticate))
                    {
                        log.Info("In Authenticate");

                        string TenantId = (requestDto as Authenticate).Meta != null ? (requestDto as Authenticate).Meta[TokenConstants.CID] : CoreConstants.EXPRESSBASE;
                        log.Info(TenantId);
                        RequestContext.Instance.Items.Add(CoreConstants.SOLUTION_ID, TenantId);
                    }
                }
                catch (Exception e)
                {
                    log.Info("ErrorStackTrace..........." + e.StackTrace);
                    log.Info("ErrorMessage..........." + e.Message);
                    log.Info("InnerException..........." + e.InnerException);
                }
                try
                {
                    if (requestDto != null && requestDto.GetType() != typeof(Authenticate) && requestDto.GetType() != typeof(GetAccessToken) && requestDto.GetType() != typeof(UniqueRequest) && requestDto.GetType() != typeof(CreateAccountRequest) /*&& requestDto.GetType() != typeof(EmailServicesMqRequest) */&& requestDto.GetType() != typeof(RegisterRequest) && requestDto.GetType() != typeof(AutoGenSidRequest) && requestDto.GetType() != typeof(JoinbetaReq) && requestDto.GetType() != typeof(GetBotsRequest)
                    && requestDto.GetType() != typeof(GetEventSubscribers) && requestDto.GetType() != typeof(GetAllFromAppStoreExternalRequest) && requestDto.GetType() != typeof(GetOneFromAppStoreRequest) && !(requestDto is EbServiceStackNoAuthRequest)/* && !(requestDto is IEbTenentRequest)*/)
                    {
                        var auth = req.Headers[HttpHeaders.Authorization];
                        if (string.IsNullOrEmpty(auth))
                            res.ReturnAuthRequired();
                        else
                        {
                            if (req.Headers[CacheConstants.RTOKEN] != null)
                            {
                                Resolve<IEbStaticFileClient>().AddAuthentication(req);
                                Resolve<IEbServerEventClient>().AddAuthentication(req);
                                Resolve<IEbMqClient>().AddAuthentication(req);

                            }

                            string solId = "";
                            int userId = 0;
                            string wc = "";
                            string sub = "";

                            if (req.Items.ContainsKey("__session"))
                            {
                                CustomUserSession csession = req.Items["__session"] as CustomUserSession;

                                solId = csession.CId;
                                userId = csession.Uid;
                                wc = csession.WhichConsole;
                                sub = csession.UserAuthId;
                            }
                            else
                            {
                                var jwtoken = new JwtSecurityToken(auth.Replace(CacheConstants.BEARER, string.Empty).Trim());
                                foreach (var c in jwtoken.Claims)
                                {
                                    if (!string.IsNullOrEmpty(c.Value))
                                    {
                                        if (c.Type == TokenConstants.CID)
                                        {
                                            solId = c.Value;
                                        }
                                        else if (c.Type == TokenConstants.UID)
                                        {
                                            userId = int.Parse(c.Value);
                                        }
                                        else if (c.Type == TokenConstants.WC)
                                        {
                                            wc = c.Value;
                                        }
                                        else if (c.Type == TokenConstants.SUB)
                                        {
                                            sub = c.Value;
                                        }
                                    }
                                }
                            }

                            RequestContext.Instance.Items.Add(CoreConstants.SOLUTION_ID, solId);
                            RequestContext.Instance.Items.Add(CoreConstants.USER_ID, userId);
                            RequestContext.Instance.Items.Add(TokenConstants.WC, wc);
                            RequestContext.Instance.Items.Add(TokenConstants.SUB, sub);


                            if (requestDto is IEbSSRequest)
                            {
                                (requestDto as IEbSSRequest).SolnId = solId;
                                (requestDto as IEbSSRequest).UserId = userId;
                            }
                            else if (requestDto is EbServiceStackAuthRequest)
                            {
                                (requestDto as EbServiceStackAuthRequest).SolnId = solId;
                                (requestDto as EbServiceStackAuthRequest).UserId = userId;
                                (requestDto as EbServiceStackAuthRequest).WhichConsole = wc;
                                (requestDto as EbServiceStackAuthRequest).UserAuthId = sub;
                            }
                            else if (requestDto is IEbTenentRequest)
                            {
                                (requestDto as IEbTenentRequest).SolnId = solId;
                                (requestDto as IEbTenentRequest).UserId = userId;
                            }


                            //foreach (var c in jwtoken.Claims)
                            //{
                            //    if (c.Type == TokenConstants.CID && !string.IsNullOrEmpty(c.Value))
                            //    {

                            //        RequestContext.Instance.Items.Add(CoreConstants.SOLUTION_ID, c.Value);
                            //        if (requestDto is IEbSSRequest)
                            //            (requestDto as IEbSSRequest).SolnId = solutionId;
                            //        if (requestDto is EbServiceStackAuthRequest)
                            //            (requestDto as EbServiceStackAuthRequest).SolnId = solutionId;
                            //        if (requestDto is IEbTenentRequest)
                            //            (requestDto as IEbTenentRequest).SolnId = solutionId;
                            //        continue;
                            //    }
                            //    if (c.Type == TokenConstants.UID && !string.IsNullOrEmpty(c.Value))
                            //    {
                            //        RequestContext.Instance.Items.Add("UserId", Convert.ToInt32(c.Value));
                            //        if (requestDto is IEbSSRequest)
                            //            (requestDto as IEbSSRequest).UserId = userId;
                            //        if (requestDto is IEbTenentRequest)
                            //            (requestDto as IEbTenentRequest).UserId = userId;
                            //        if (requestDto is EbServiceStackAuthRequest)
                            //            (requestDto as EbServiceStackAuthRequest).UserId = userId;
                            //        continue;
                            //    }
                            //    if (c.Type == TokenConstants.WC && !string.IsNullOrEmpty(c.Value))
                            //    {
                            //        RequestContext.Instance.Items.Add(TokenConstants.WC, c.Value);
                            //        if (requestDto is EbServiceStackAuthRequest)
                            //            (requestDto as EbServiceStackAuthRequest).WhichConsole = c.Value.ToString();
                            //        continue;
                            //    }
                            //    if (c.Type == TokenConstants.SUB && !string.IsNullOrEmpty(c.Value))
                            //    {
                            //        RequestContext.Instance.Items.Add(TokenConstants.SUB, c.Value);
                            //        if (requestDto is EbServiceStackAuthRequest)
                            //            (requestDto as EbServiceStackAuthRequest).UserAuthId = c.Value.ToString();
                            //        continue;
                            //    }
                            //}
                        }
                    }
                    else if (requestDto.GetType() == typeof(GetBotsRequest))
                    {
                        string x = req.Headers["SolId"].ToString();
                        if (!String.IsNullOrEmpty(x))
                        {
                            RequestContext.Instance.Items.Add(CoreConstants.SOLUTION_ID, x);
                            if (requestDto is IEbSSRequest)
                                (requestDto as IEbSSRequest).SolnId = x;
                            if (requestDto is EbServiceStackAuthRequest)
                                (requestDto as EbServiceStackAuthRequest).SolnId = x;
                        }
                    }
                }
                catch (Exception e)
                {
                    log.Info("ErrorStackTraceNontokenServices..........." + e.StackTrace);
                    log.Info("ErrorMessageNontokenServices..........." + e.Message);
                    log.Info("InnerExceptionNontokenServices..........." + e.InnerException);
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

            this.GlobalRequestFilters.Add((req, res, requestDto) =>
            {
                if (req.RawUrl.Contains("smscallback"))
                {
                    req.Headers.Add("BearerToken", "");

                }
            });

            //--Api Key Generation
            //AfterInitCallbacks.Add(host =>
            //{

            //    var authProvider = (ApiKeyAuthProvider)AuthenticateService.GetAuthProvider(ApiKeyAuthProvider.Name);
            //    var authRepo = (IManageApiKeys)host.TryResolve<IAuthRepository>();
            //    var userRepo = (IUserAuthRepository)host.TryResolve<IUserAuthRepository>();

            //    try
            //    {
            //        IEnumerable<ApiKey> keys = authProvider.GenerateNewApiKeys("62");
            //        authRepo.StoreAll(keys);

            //    }
            //    catch (Exception e)
            //    {
            //        throw;
            //    }

            //});
        }
    }

}
