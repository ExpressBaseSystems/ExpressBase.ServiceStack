using ExpressBase.Objects.ServiceStack_Artifacts;
using MailKit.Net.Smtp;
using MailKit.Security;
using MimeKit;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class EmailServices : EbBaseService
    {
        [Authenticate]
        public async Task<EmailServicesResponse> Any(EmailServicesRequest request)
        {
            var emailMessage = new MimeMessage();

            emailMessage.From.Add(new MailboxAddress("EXPRESSbase", "info@expressbase.com"));
            emailMessage.To.Add(new MailboxAddress("", request.To));
            emailMessage.Subject = request.Subject;
            emailMessage.Body = new TextPart("plain") { Text = request.Message };
            try
            {
                using (var client = new SmtpClient())
                {
                    client.LocalDomain = "www.expressbase.com";
                    await client.ConnectAsync("smtp.gmail.com", 465, true).ConfigureAwait(false); 
                    await client.AuthenticateAsync(new System.Net.NetworkCredential() { UserName = "binimvarghese@gmail.com", Password = "nibibininini" }).ConfigureAwait(false);
                    await client.SendAsync(emailMessage).ConfigureAwait(false);
                    await client.DisconnectAsync(true).ConfigureAwait(false);
                }
                return new EmailServicesResponse
                {
                    Success = true
                };
            }
            catch(Exception e)
            {
                return new EmailServicesResponse
                {
                    Success = false
                    
                };

            }
        }
    }
}
