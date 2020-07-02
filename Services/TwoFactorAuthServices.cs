using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Microsoft.IdentityModel.Tokens;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Text;
using ExpressBase.Security;
using ServiceStack.Auth;
using ExpressBase.Common.LocationNSolution;
using ExpressBase.Common.Constants;
using ExpressBase.ServiceStack.MQServices;
using System.Security.Principal;

namespace ExpressBase.ServiceStack.Services
{

    public class TwoFactorAuthServices : EbBaseService
    {
        public TwoFactorAuthServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        public const string OtpMessage = "One-Time Password for log in to {0} is {1}. Do not share with anyone. This OTP is valid for 5 minutes.";
        public MyAuthenticateResponse MyAuthenticateResponse { get; set; }

        public Authenticate2FAResponse AuthResponse { get; set; }

        public Authenticate2FAResponse Post(Authenticate2FARequest request)
        {
            AuthResponse = new Authenticate2FAResponse();
            this.MyAuthenticateResponse = request.MyAuthenticateResponse;
            AuthResponse.Is2fa = true;
            AuthResponse.AuthStatus = true;
            Eb_Solution sol_Obj = GetSolutionObject(request.SolnId);
            string otp = GenerateOTP();
            User _usr = SetUserObj(otp); // updating otp and tokens in redis userobj
            AuthResponse.TwoFAToken = GenerateToken();
            SendOtp(sol_Obj, _usr);
            return AuthResponse;
        }

        public Authenticate2FAResponse Post(Validate2FARequest request)
        {
            AuthResponse = new Authenticate2FAResponse();
            AuthResponse.AuthStatus = ValidateToken(request.Token, request.UserAuthId);
            if (!AuthResponse.AuthStatus)
            {
                AuthResponse.ErrorMessage = "Something went wrong with token";
            }
            return AuthResponse;
        }

        public Authenticate2FAResponse Post(ResendOTP2FARequest request)
        {
            AuthResponse = new Authenticate2FAResponse();
            AuthResponse.AuthStatus = ValidateToken(request.Token, request.UserAuthId);
            if (AuthResponse.AuthStatus)
            {
                Eb_Solution sol_Obj = GetSolutionObject(request.SolnId);
                User _usr = this.Redis.Get<User>(request.UserAuthId);
                SendOtp(sol_Obj, _usr);
            }
            else
            {
                AuthResponse.ErrorMessage = "Something went wrong with token";
            }
            return AuthResponse;
        }

        public bool ValidateToken(string authToken, string userAuthId)
        {
            bool status = false;
            JwtSecurityTokenHandler tokenHandler = new JwtSecurityTokenHandler();
            TokenValidationParameters validationParameters = GetValidationParameters();
            try
            {
                IPrincipal principal = tokenHandler.ValidateToken(authToken, validationParameters, out SecurityToken validatedToken);
                var token = tokenHandler.ReadJwtToken(authToken);
                string value = "";
                ((List<Claim>)token.Claims).ForEach(a => { if (a.Type == "AuthId") value = a.Value; });
                if (value != userAuthId)
                    throw new Exception();
                status = true;
            }
            catch
            {
                Console.WriteLine("Token validation failed :: invalid token");
            }
            return status;
        }

        private TokenValidationParameters GetValidationParameters()
        {
            return new TokenValidationParameters()
            {
                ValidateIssuerSigningKey = true,
                ValidateAudience = false,
                ValidateActor = false,
                ValidateIssuer = false,
                ValidateLifetime = true,
                ClockSkew = TimeSpan.Zero,
                IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(Environment.GetEnvironmentVariable(EnvironmentConstants.EB_JWT_PRIVATE_KEY_XML))) // The same key as the one that generate the token
            };
        }

        private string GenerateOTP()
        {
            string sOTP = String.Empty;
            string sTempChars = String.Empty;
            int iOTPLength = 6;
            string[] saAllowedCharacters = { "1", "2", "3", "4", "5", "6", "7", "8", "9", "0" };
            Random rand = new Random();
            for (int i = 0; i < iOTPLength; i++)
            {
                int p = rand.Next(0, saAllowedCharacters.Length);
                sTempChars = saAllowedCharacters[rand.Next(0, saAllowedCharacters.Length)];
                sOTP += sTempChars;
            }
            return sOTP;
        }

        public string GenerateToken()
        {
            try
            {
                JwtSecurityTokenHandler tokenHandler = new JwtSecurityTokenHandler();
                byte[] key = Encoding.ASCII.GetBytes(Environment.GetEnvironmentVariable(EnvironmentConstants.EB_JWT_PRIVATE_KEY_XML));
                SecurityTokenDescriptor tokenDescriptor = new SecurityTokenDescriptor
                {
                    Subject = new System.Security.Claims.ClaimsIdentity(
                        new Claim[] {
                        new Claim("AuthId", this.MyAuthenticateResponse.User.AuthId),
                        }),
                    Expires = DateTime.UtcNow.AddMinutes(5),
                    SigningCredentials = new SigningCredentials(new SymmetricSecurityKey(key), SecurityAlgorithms.HmacSha256Signature),
                };
                SecurityToken token = tokenHandler.CreateToken(tokenDescriptor);
                return tokenHandler.WriteToken(token);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
            return null;
        }

        private User SetUserObj(string otp)
        {
            User u = this.Redis.Get<User>(this.MyAuthenticateResponse.User.AuthId);
            u.Otp = otp;
            u.BearerToken = this.MyAuthenticateResponse.BearerToken;
            u.RefreshToken = this.MyAuthenticateResponse.RefreshToken;
            this.Redis.Set<IUserAuth>(this.MyAuthenticateResponse.User.AuthId, u);// must set as IUserAuth
            return u;
        }

        public void SendOtp(Eb_Solution sol_Obj, User _usr)
        {
            if (sol_Obj.OtpDelivery != null)
            {
                try
                {
                    string[] _otpmethod = sol_Obj.OtpDelivery.Split(",");
                    if (_otpmethod[0] == "email")
                    {
                        if (!string.IsNullOrEmpty(_usr.Email))
                        {
                            SendOtpEmail(_usr, sol_Obj);
                            AuthResponse.OtpTo = _usr.Email;
                        }
                        else
                        {
                            AuthResponse.AuthStatus = false;
                            AuthResponse.ErrorMessage = "Email id not set for the user. Please contact your admin";
                        }
                    }
                    else if (_otpmethod[0] == "sms")
                    {
                        if (!string.IsNullOrEmpty(_usr.PhoneNumber))
                        {
                            string lastDigit = _usr.PhoneNumber.Substring((_usr.PhoneNumber.Length - 4), 4);
                            SendOtpSms(_usr, sol_Obj);
                            AuthResponse.OtpTo = "******" + lastDigit;
                        }
                        else
                        {
                            AuthResponse.AuthStatus = false;
                            AuthResponse.ErrorMessage = "Phone number not set for the user. Please contact your admin";
                        }
                    }
                }
                catch (Exception e)
                {
                    AuthResponse.AuthStatus = false;
                    AuthResponse.ErrorMessage = e.Message;
                }
            }
            else
            {
                AuthResponse.AuthStatus = false;
                AuthResponse.ErrorMessage = "Otp delivery method not set.";
            }
        }

        public void SendOtpEmail(User _usr, Eb_Solution soln)
        {
            string message = string.Format(OtpMessage, soln.ExtSolutionID, _usr.Otp);
            EmailService emailService = base.ResolveService<EmailService>();
            emailService.Post(new EmailDirectRequest
            {
                To = _usr.Email,
                Subject = "OTP Verification",
                Message = message,
                SolnId = soln.SolutionID,
                UserId = _usr.UserId,
                WhichConsole = TokenConstants.UC,
                UserAuthId = _usr.AuthId
            });
        }

        public void SendOtpSms(User _usr, Eb_Solution soln)
        {
            string message = string.Format(OtpMessage, soln.ExtSolutionID, _usr.Otp);
            SmsCreateService smsCreateService = base.ResolveService<SmsCreateService>();
            smsCreateService.Post(new SmsDirectRequest
            {
                To = _usr.PhoneNumber,
                Body = message,
                SolnId = soln.SolutionID,
                UserId = _usr.UserId,
                WhichConsole = TokenConstants.UC,
                UserAuthId = _usr.AuthId
            });
        }
    }
}
