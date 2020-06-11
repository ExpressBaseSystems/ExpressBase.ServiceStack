using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.EbServiceStack.ReqNRes;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.ServiceStack.Auth;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.ServiceStack.Auth0;
using Funq;
using iTextSharp.text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.RabbitMq;
using ServiceStack.Redis;
using System;
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
                    payload[TokenConstants.IP] = (session as CustomUserSession).SourceIp;
                },

                PopulateSessionFilter = (session, token, req) =>
                {
                    var csession = session as CustomUserSession;
                    csession.UserAuthId = token[TokenConstants.SUB];
                    csession.CId = token[TokenConstants.CID];
                    csession.Uid = Convert.ToInt32(token[TokenConstants.UID]);
                    csession.WhichConsole = token[TokenConstants.WC];
                    csession.SourceIp = token[TokenConstants.IP];
                }
            };

            EbApiAuthProvider apiprovider = new EbApiAuthProvider(AppSettings)
            {
#if (DEBUG)
                RequireSecureConnection = false,
                //EncryptPayload = true,
#endif
            };

            string env = Environment.GetEnvironmentVariable(EnvironmentConstants.ASPNETCORE_ENVIRONMENT);

            string fburl = "";

            if (env == "Staging")
            {
                fburl = "https://ss.eb-test.xyz/auth/facebook";
            }
            else if (env == "Production")
            {
                fburl = "https://ss.expressbase.com/auth/facebook";
            }
            else
            {
                fburl = "http://localhost:41600/auth/facebook";
            }

            //MyFacebookAuthProvider fbauth = new MyFacebookAuthProvider(AppSettings)
            //{
            //	//AppId = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_FB_APP_ID),
            //	//AppSecret = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_FB_APP_SECRET),
            //	//Permissions = new string[] { "email, public_profile, user_hometown" },
            //	//RedirectUrl = fburl

            //	AppId = "149537802493867",
            //	AppSecret = "55a9b5e0a88089465808bdc1d4f07e8e",
            //	Permissions = new string[] { "email, public_profile, user_hometown" },
            //	RedirectUrl = fburl
            //};


            this.Plugins.Add(new CorsFeature(allowedHeaders: "Content-Type, Authorization, Access-Control-Allow-Origin, Access-Control-Allow-Credentials"));

            //this.Plugins.Add(new ProtoBufFormat());
            this.Plugins.Add(new SessionFeature());

            this.Plugins.Add(new AuthFeature(() =>
                new CustomUserSession(),
                new IAuthProvider[]
                {
                    new MyCredentialsAuthProvider(AppSettings) { PersistSession = true },
                    jwtprovider,
                    //fbauth,
                    //apiprovider,
                    new MyTwitterAuthProvider(AppSettings)
                    {
                        ConsumerKey = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_TWITTER_CONSUMER_KEY),
                        ConsumerSecret = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_TWITTER_CONSUMER_SECRET),
                        //Need to Change 
                        CallbackUrl = "http://localhost:8000/auth/twitter",
                        RequestTokenUrl= "https://api.twitter.com/oauth/authenticate",
                    },
					//new MyFacebookAuthProvider(AppSettings)
					//{
					//	//febin
					//	//AppId = "149537802493867",
					//	 //AppSecret = "55a9b5e0a88089465808bdc1d4f07e8e",
						
					//	  //unni
					//	  //AppId = "628799957635144",
					//	 // AppSecret = "abf6b5ad5f0f2b886ccaeddc72f209c2",

					//	  AppId = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_FB_APP_ID),
					//	  AppSecret = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_FB_APP_SECRET),
					//	  Permissions = new string[] { "email, public_profile, user_hometown" },
					//},

					new MyGithubAuthProvider(AppSettings)
                    {
                        ClientId =Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GITHUB_CLIENT_ID),
                        ClientSecret = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GITHUB_CLIENT_SECRET)

							//ClientId ="de0c8eefca9c1871a521",
							//ClientSecret = "805bf067aa1768e1d63bc4f540d0f79834a3955f"
					}


                }));

            this.ContentTypes.Register(MimeTypes.ProtoBuf, (reqCtx, res, stream) => ProtoBuf.Serializer.NonGeneric.Serialize(stream, res), ProtoBuf.Serializer.NonGeneric.Deserialize);

#if (DEBUG)
            SetConfig(new HostConfig { DebugMode = true });
#endif
            SetConfig(new HostConfig { DefaultContentType = MimeTypes.Json });
            var redisServer = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_REDIS_SERVER);
            //if (true)
            //{
            //    container.Register<IRedisClientsManager>(c => new RedisManagerPool("34.93.50.143"));
            //}
            //else
            //if (env == "Staging")
            //{
            //    container.Register<IRedisClientsManager>(c => new RedisManagerPool(redisServer));
            //}
            //else
            //{
                var redisPassword = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_REDIS_PASSWORD);
                var redisPort = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_REDIS_PORT);
                var redisConnectionString = string.Format("redis://{0}@{1}:{2}", redisPassword, redisServer, redisPort);
                container.Register<IRedisClientsManager>(c => new RedisManagerPool(redisConnectionString));

            //}
            container.Register<IAuthRepository>(c => new MyRedisAuthRepository(c.Resolve<IRedisClientsManager>()));
            //container.Register<IManageApiKeys>(c => new EbApiRedisAuthRepository(c.Resolve<IRedisClientsManager>()));

            container.Register(c => c.Resolve<IRedisClientsManager>().GetCacheClient());
            container.Register<JwtAuthProvider>(jwtprovider);
            container.Register<IEbConnectionFactory>(c => new EbConnectionFactory(c)).ReusedWithin(ReuseScope.Request);
            container.Register<IEbServerEventClient>(c => new EbServerEventClient()).ReusedWithin(ReuseScope.Request);
            container.Register<IEbMqClient>(c => new EbMqClient()).ReusedWithin(ReuseScope.Request);
            container.Register<IEbStaticFileClient>(c => new EbStaticFileClient()).ReusedWithin(ReuseScope.Request);


            //Plugins.Add(new RedisServiceDiscoveryFeature());


            // Setting Assembly version in Redis
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
            mqServer.RegisterHandler<EmailAttachmentRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<ExportApplicationRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<ImportApplicationRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<SMSPrepareRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<ReportInternalRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<AddSchedulesToSolutionRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<ExportToExcelServiceRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<UpdateSidMapMqRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<UpdateRedisConnectionsMqRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<SlackCreateRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<SqlJobInternalRequest>(base.ExecuteMessage);

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

                log.Info(string.Format("Started Execution of {0} at {1}", requestDto.GetType().ToString(), DateTime.Now.TimeOfDay));

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
                    if (requestDto != null && requestDto.GetType() != typeof(Authenticate) && requestDto.GetType() != typeof(GetAccessToken) && requestDto.GetType() != typeof(UniqueRequest) /*&& requestDto.GetType() != typeof(EmailServicesMqRequest) */&& requestDto.GetType() != typeof(RegisterRequest) && requestDto.GetType() != typeof(JoinbetaReq) && requestDto.GetType() != typeof(GetBotsRequest)
                    && requestDto.GetType() != typeof(GetEventSubscribers) && requestDto.GetType() != typeof(GetAllFromAppStoreExternalRequest) &&
                    requestDto.GetType() != typeof(GetOneFromAppStoreRequest) && !(requestDto is EbServiceStackNoAuthRequest) && !(requestDto is UpdateSidMapRequest)
                    && !(requestDto is IoTDataRequest)/* && !(requestDto is IEbTenentRequest)*/)
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

                            string solId = string.Empty;
                            int userId = 0;
                            string wc = string.Empty;
                            string sub = string.Empty;

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
                                //(requestDto as IEbTenentRequest).SolnId = solId;
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
                ILog log = LogManager.GetLogger(GetType());

                if (responseDto != null)
                    log.Info(string.Format("Finished Execution of {0} at {1}", responseDto.GetType().ToString(), DateTime.Now.TimeOfDay));

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
            try
            {
                RegisterFont();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
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
        public void RegisterFont()
        {
            Dictionary<string, string> FontPaths = new Dictionary<string, string>();
            FontPaths.Add("Century Gothic", "07558_centurygothic.ttf");
            foreach (KeyValuePair<string, string> _fonts in FontPaths)
            {
                if (!FontFactory.IsRegistered(_fonts.Key))
                {
                    try
                    {
                        FontFactory.Register(_fonts.Value);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }
            }
        }
    }

}
