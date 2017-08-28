using ExpressBase.Objects.Objects.MQRelated;
using MailKit.Net.Smtp;
using MimeKit;
using ServiceStack;

namespace ExpressBase.ServiceStack
{
    public class EmailService : Service
    {
        public object Any(EmailRequest req)
        {

            string FromAddress = "ahammedunni@expressbase.com";
            string FromAdressTitle = req.FromAdressTitle;
            //To Address  
            string ToAddress = req.ToAddress;
            string ToAdressTitle = req.ToAdressTitle;
            string Subject = req.Subject;
            string BodyContent = req.BodyContent;

            string SmtpServer = "smtp.zoho.com";

            //Smtp Port Number  
            int SmtpPortNumber = 465;
            var mimeMessage = new MimeMessage();
            mimeMessage.From.Add(new MailboxAddress(FromAdressTitle, FromAddress));
            mimeMessage.To.Add(new MailboxAddress(ToAdressTitle, ToAddress));
            mimeMessage.Subject = Subject;
            mimeMessage.Body = new TextPart("plain")
            {
                Text = BodyContent
            };
            using (var client = new SmtpClient())
            {

                client.Connect(SmtpServer, SmtpPortNumber, true);
                // Note: only needed if the SMTP server requires authentication  
                // Error 5.5.1 Authentication   
                client.Authenticate("ahammedunni@expressbase.com", "ZMorp2yaW@ZM");
                client.Send(mimeMessage);
                client.Disconnect(true);
            }

            return null;
        }
    }

}
