using MailKit.Net.Smtp;
using MimeKit;
using ServiceStack;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    public class EmailService : Service
    {
        public async Task<object> PostAsync(EmailServicesMqRequest request)
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
                    await client.AuthenticateAsync(new System.Net.NetworkCredential() { UserName = "expressbasesystems@gmail.com", Password = "ebsystems" }).ConfigureAwait(false);
                    await client.SendAsync(emailMessage).ConfigureAwait(false);
                    await client.DisconnectAsync(true).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                return e;
            }
            return null;
        }
    }
}
