using ServiceStack;
using ServiceStack.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class ServerEventsSSServices
    {
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
                    var subscriptionInfos = ServerEvents.GetSubscriptionInfosByUserId("eb_dbpjl5pgxleq20180130063835-binivarghese@gmail.com-uc");
                    //ServerEvents.NotifyUserId("4545", "cmd.notify", msg);
                    foreach(var x in subscriptionInfos)
                        ServerEvents.NotifySubscription(x.SubscriptionId, "cmd.notify", msg);
                    
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
}
