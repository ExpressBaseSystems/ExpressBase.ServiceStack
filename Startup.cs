using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.EbServiceStack.ReqNRes;
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
using ServiceStack.Configuration;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.ProtoBuf;
using ServiceStack.RabbitMq;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Net;

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

        public AppHost() : base("Test Razor", typeof(AppHost).GetAssembly()) { }

        public override void OnAfterConfigChanged()
        {
            base.OnAfterConfigChanged();
        }

        public override void Configure(Container container)
        {
            var co = this.Config;
            LogManager.LogFactory = new ConsoleLogFactory(debugEnabled: true);

            var jwtprovider = new JwtAuthProvider
            {
                HashAlgorithm = "RS256",
                PrivateKeyXml = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_JWT_PRIVATE_KEY_XML),
                PublicKeyXml = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_JWT_PUBLIC_KEY_XML),
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
//            var apikeyauthprovider = new ApiKeyAuthProvider(AppSettings)
//            {
//#if (DEBUG)
//                RequireSecureConnection = false,
//                //EncryptPayload = true,
//#endif
//                PersistSession = true,
//                SessionExpiry = TimeSpan.FromHours(12)
//            };

            this.Plugins.Add(new CorsFeature(allowedHeaders: "Content-Type, Authorization, Access-Control-Allow-Origin, Access-Control-Allow-Credentials"));
            this.Plugins.Add(new ProtoBufFormat());
            this.Plugins.Add(new ServerEventsFeature());

            this.Plugins.Add(new AuthFeature(() => new CustomUserSession(),
                new IAuthProvider[] {
                    new MyFacebookAuthProvider(AppSettings)
                    {
                        AppId = "151550788692231",
                        AppSecret = "94ec1a04342e5cf7e7a971f2eb7ad7bc",
                        Permissions = new string[] { "email, public_profile" }
                    },

                    new MyTwitterAuthProvider(AppSettings)
                    {
                        ConsumerKey = "6G9gaYo7DMx1OHYRAcpmkPfvu",
                        ConsumerSecret = "Jx8uUIPeo5D0agjUnqkKHGQ4o6zTrwze9EcLtjDlOgLnuBaf9x",
                       // CallbackUrl = "http://localhost:8000/auth/twitter",
                        
                       // RequestTokenUrl= "https://api.twitter.com/oauth/authenticate",
                        
                    },

                    new MyGithubAuthProvider(AppSettings)
                    {
                    ClientId="4504eefeb8f027c810dd",
                    ClientSecret="d9c1c956a9fddd089798e0031851e93a8d0e5cc6",
                    RedirectUrl ="http://localhost:8000/"
                    },

                    new MyCredentialsAuthProvider(AppSettings)
                    {
                        PersistSession = true
                    },

                    jwtprovider,
                    //apikeyauthprovider

                }));

            //Also works but it's recommended to handle 404's by registering at end of .NET Core pipeline
            //this.CustomErrorHttpHandlers[HttpStatusCode.NotFound] = new RazorHandler("/notfound");

            this.ContentTypes.Register(MimeTypes.ProtoBuf, (reqCtx, res, stream) => ProtoBuf.Serializer.NonGeneric.Serialize(stream, res), ProtoBuf.Serializer.NonGeneric.Deserialize);

            SetConfig(new HostConfig { DebugMode = true });
            SetConfig(new HostConfig { DefaultContentType = MimeTypes.Json });

            var redisConnectionString = string.Format("redis://{0}@{1}:{2}",
               Environment.GetEnvironmentVariable(EnvironmentConstants.EB_REDIS_PASSWORD),
               Environment.GetEnvironmentVariable(EnvironmentConstants.EB_REDIS_SERVER),
               Environment.GetEnvironmentVariable(EnvironmentConstants.EB_REDIS_PORT));

            container.Register<IRedisClientsManager>(c => new RedisManagerPool(redisConnectionString));
            container.Register<IUserAuthRepository>(c => new EbRedisAuthRepository(c.Resolve<IRedisClientsManager>()));

            container.Register<JwtAuthProvider>(jwtprovider);
            container.RegisterAutoWiredAs<MemoryChatHistory, IChatHistory>();

            container.Register<IServerEvents>(c => new RedisServerEvents(c.Resolve<IRedisClientsManager>()));
            container.Resolve<IServerEvents>().Start();

            //container.Register<ApiKeyAuthProvider>(apikeyauthprovider);

            container.Register<IEbConnectionFactory>(c => new EbConnectionFactory(c)).ReusedWithin(ReuseScope.Request);

            RabbitMqMessageFactory rabitFactory = new RabbitMqMessageFactory();
            rabitFactory.ConnectionFactory.UserName = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_USER);
            rabitFactory.ConnectionFactory.Password = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_PASSWORD);
            rabitFactory.ConnectionFactory.HostName = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_HOST);
            rabitFactory.ConnectionFactory.Port = Convert.ToInt32(Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_PORT));
            rabitFactory.ConnectionFactory.VirtualHost = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_VHOST);

            var mqServer = new RabbitMqServer(rabitFactory);
            mqServer.RetryCount = 1;
            //mqServer.RegisterHandler<EmailServicesMqRequest>(base.ExecuteMessage);
            //mqServer.RegisterHandler<SMSSentMqRequest>(base.ExecuteMessage);
            //mqServer.RegisterHandler<RefreshSolutionConnectionsMqRequest>(base.ExecuteMessage);
            //mqServer.RegisterHandler<SMSStatusLogMqRequest>(base.ExecuteMessage);
            //mqServer.RegisterHandler<UploadFileMqRequest>(base.ExecuteMessage);
            //mqServer.RegisterHandler<ImageResizeMqRequest>(base.ExecuteMessage);
            //mqServer.RegisterHandler<FileMetaPersistMqRequest>(base.ExecuteMessage);
            //mqServer.RegisterHandler<SlackPostMqRequest>(base.ExecuteMessage);
            //mqServer.RegisterHandler<SlackAuthMqRequest>(base.ExecuteMessage);

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
                ILog log = LogManager.GetLogger(GetType());

                log.Info("In GlobalRequestFilters");
                try
                {
                    if (requestDto.GetType() == typeof(Authenticate))
                    {
                        log.Info("In Authenticate");

                        string TenantId = (requestDto as Authenticate).Meta != null ? (requestDto as Authenticate).Meta["cid"] : CoreConstants.EXPRESSBASE;
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
                    if (requestDto != null && requestDto.GetType() != typeof(Authenticate) && requestDto.GetType() != typeof(GetAccessToken) && requestDto.GetType() != typeof(UniqueRequest) && requestDto.GetType() != typeof(CreateAccountRequest)&& requestDto.GetType() != typeof(EmailServicesMqRequest) && requestDto.GetType() != typeof(RegisterRequest) && requestDto.GetType() != typeof(AutoGenEbIdRequest)
                    && requestDto.GetType() != typeof(GetEventSubscribers) && requestDto.GetType() != typeof(GetChatHistory) && requestDto.GetType() != typeof(PostChatToChannel))
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
                                    RequestContext.Instance.Items.Add(CoreConstants.SOLUTION_ID, c.Value);
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

    public interface IChatHistory
    {
        long GetNextMessageId(string channel);

        void Log(string channel, ChatMessage msg);

        List<ChatMessage> GetRecentChatHistory(string channel, long? afterId, int? take);

        void Flush();
    }

    public class MemoryChatHistory : IChatHistory
    {
        public int DefaultLimit { get; set; }

        public IServerEvents ServerEvents { get; set; }

        public MemoryChatHistory()
        {
            DefaultLimit = 100;
        }

        Dictionary<string, List<ChatMessage>> MessagesMap = new Dictionary<string, List<ChatMessage>>();

        public long GetNextMessageId(string channel)
        {
            return ServerEvents.GetNextSequence("chatMsg");
        }

        public void Log(string channel, ChatMessage msg)
        {
            List<ChatMessage> msgs;
            if (!MessagesMap.TryGetValue(channel, out msgs))
                MessagesMap[channel] = msgs = new List<ChatMessage>();

            msgs.Add(msg);
        }

        public List<ChatMessage> GetRecentChatHistory(string channel, long? afterId, int? take)
        {
            List<ChatMessage> msgs;
            if (!MessagesMap.TryGetValue(channel, out msgs))
                return new List<ChatMessage>();

            var ret = msgs.Where(x => x.Id > afterId.GetValueOrDefault())
                          .Reverse()  //get latest logs
                          .Take(take.GetValueOrDefault(DefaultLimit))
                          .Reverse(); //reverse back

            return ret.ToList();
        }

        public void Flush()
        {
            MessagesMap = new Dictionary<string, List<ChatMessage>>();
        }
    }

    [Route("/channels/{Channel}/chat")]
    public class PostChatToChannel : IReturn<ChatMessage>
    {
        public string From { get; set; }
        public string ToUserId { get; set; }
        public string Channel { get; set; }
        public string Message { get; set; }
        public string Selector { get; set; }
    }

    public class ChatMessage
    {
        public long Id { get; set; }
        public string Channel { get; set; }
        public string FromUserId { get; set; }
        public string FromName { get; set; }
        public string DisplayName { get; set; }
        public string Message { get; set; }
        public string UserAuthId { get; set; }
        public bool Private { get; set; }
    }

    [Route("/channels/{Channel}/raw")]
    public class PostRawToChannel : IReturnVoid
    {
        public string From { get; set; }
        public string ToUserId { get; set; }
        public string Channel { get; set; }
        public string Message { get; set; }
        public string Selector { get; set; }
    }

    [Route("/chathistory")]
    public class GetChatHistory : IReturn<GetChatHistoryResponse>
    {
        public string[] Channels { get; set; }
        public long? AfterId { get; set; }
        public int? Take { get; set; }
    }

    public class GetChatHistoryResponse
    {
        public List<ChatMessage> Results { get; set; }
        public ResponseStatus ResponseStatus { get; set; }
    }

    [Route("/reset")]
    public class ClearChatHistory : IReturnVoid { }

    [Route("/reset-serverevents")]
    public class ResetServerEvents : IReturnVoid { }

    [Route("/channels/{Channel}/object")]
    public class PostObjectToChannel : IReturnVoid
    {
        public string ToUserId { get; set; }
        public string Channel { get; set; }
        public string Selector { get; set; }

        public CustomType CustomType { get; set; }
        public SetterType SetterType { get; set; }
    }
    public class CustomType
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }
    public class SetterType
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    public class ServerEventsServices : Service
    {
        public IServerEvents ServerEvents { get; set; }
        public IChatHistory ChatHistory { get; set; }
        public IAppSettings AppSettings { get; set; }

        public void Any(PostRawToChannel request)
        {
            if (!IsAuthenticated && AppSettings.Get("LimitRemoteControlToAuthenticatedUsers", false))
                throw new HttpError(HttpStatusCode.Forbidden, "You must be authenticated to use remote control.");

            // Ensure the subscription sending this notification is still active
            var sub = ServerEvents.GetSubscriptionInfo(request.From);
            if (sub == null)
                throw HttpError.NotFound($"Subscription {request.From} does not exist");

            // Check to see if this is a private message to a specific user
            var msg = PclExportClient.Instance.HtmlEncode(request.Message);
            if (request.ToUserId != null)
            {
                // Only notify that specific user
                ServerEvents.NotifyUserId(request.ToUserId, request.Selector, msg);
            }
            else
            {
                // Notify everyone in the channel for public messages
                ServerEvents.NotifyChannel(request.Channel, request.Selector, msg);
            }
        }

        public object Any(PostChatToChannel request)
        {
            // Ensure the subscription sending this notification is still active
            var sub = ServerEvents.GetSubscriptionInfo(request.From);
            if (sub == null)
                throw HttpError.NotFound("Subscription {0} does not exist".Fmt(request.From));

            var channel = request.Channel;

            // Create a DTO ChatMessage to hold all required info about this message
            var msg = new ChatMessage
            {
                Id = ChatHistory.GetNextMessageId(channel),
                Channel = request.Channel,
                FromUserId = sub.UserId,
                FromName = sub.DisplayName,
                Message = PclExportClient.Instance.HtmlEncode(request.Message),
            };

            // Check to see if this is a private message to a specific user
            if (request.ToUserId != null)
            {
                // Mark the message as private so it can be displayed differently in Chat
                msg.Private = true;
                // Send the message to the specific user Id
                ServerEvents.NotifyUserId(request.ToUserId, request.Selector, msg);

                // Also provide UI feedback to the user sending the private message so they
                // can see what was sent. Relay it to all senders active subscriptions 
                var toSubs = ServerEvents.GetSubscriptionInfosByUserId(request.ToUserId);
                foreach (var toSub in toSubs)
                {
                    // Change the message format to contain who the private message was sent to
                    msg.Message = $"@{toSub.DisplayName}: {msg.Message}";
                    ServerEvents.NotifySubscription(request.From, request.Selector, msg);
                }
            }
            else
            {
                // Notify everyone in the channel for public messages
                ServerEvents.NotifyChannel(request.Channel, request.Selector, msg);
            }

            if (!msg.Private)
                ChatHistory.Log(channel, msg);

            return msg;
        }

        public object Any(GetChatHistory request)
        {
            var msgs = request.Channels.Map(x =>
                ChatHistory.GetRecentChatHistory(x, request.AfterId, request.Take))
                .SelectMany(x => x)
                .OrderBy(x => x.Id)
                .ToList();

            return new GetChatHistoryResponse
            {
                Results = msgs
            };
        }

        public object Any(ClearChatHistory request)
        {
            ChatHistory.Flush();
            return HttpResult.Redirect("/");
        }

        public void Any(ResetServerEvents request)
        {
            ServerEvents.Reset();
        }

        public void Any(PostObjectToChannel request)
        {
            if (request.ToUserId != null)
            {
                if (request.CustomType != null)
                    ServerEvents.NotifyUserId(request.ToUserId, request.Selector ?? Selector.Id<CustomType>(), request.CustomType);
                if (request.SetterType != null)
                    ServerEvents.NotifyUserId(request.ToUserId, request.Selector ?? Selector.Id<SetterType>(), request.SetterType);
            }
            else
            {
                if (request.CustomType != null)
                    ServerEvents.NotifyChannel(request.Channel, request.Selector ?? Selector.Id<CustomType>(), request.CustomType);
                if (request.SetterType != null)
                    ServerEvents.NotifyChannel(request.Channel, request.Selector ?? Selector.Id<SetterType>(), request.SetterType);
            }
        }
    }

    [Route("/account")]
    public class GetUserDetails { }

    public class GetUserDetailsResponse2
    {
        public string Provider { get; set; }
        public string UserId { get; set; }
        public string UserName { get; set; }
        public string FullName { get; set; }
        public string DisplayName { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Company { get; set; }
        public string Email { get; set; }
        public string PhoneNumber { get; set; }

        public DateTime? BirthDate { get; set; }
        public string BirthDateRaw { get; set; }
        public string Address { get; set; }
        public string Address2 { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string Country { get; set; }
        public string Culture { get; set; }
        public string Gender { get; set; }
        public string Language { get; set; }
        public string MailAddress { get; set; }
        public string Nickname { get; set; }
        public string PostalCode { get; set; }
        public string TimeZone { get; set; }
    }

    //[Authenticate]
    public class UserDetailsService : Service
    {
        public object Get(GetUserDetails request)
        {
            var session = GetSession();
            return session.ConvertTo<GetUserDetailsResponse2>();
        }
    }
}
