using ServiceStack;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Models
{
    public class Registermodel
    {
        [Required]
        [Display(Name = "User name")]
        public string UserName { get; set; }
        [Required]
        [DataType(DataType.Password)]
        [Display(Name = "Password")]
        public string Password { get; set; }
        [Required]
        [Display(Name = "First name")]
        public string FirstName { get; set; }
        [Required]
        [Display(Name = "Last name")]
        public string LastName { get; set; }
        [Required]
        [Display(Name = "Middle name")]
        public string MiddleName { get; set; }
        [Required]
        [Display(Name = "DOB")]
        public string DOB { get; set; }
        [Required]
        [Display(Name = "PhNoPrimary")]
        public string PhNoPrimary { get; set; }
        [Required]
        [Display(Name = "PhNoSecondary")]
        public string PhNoSecondary { get; set; }
        [Required]
        [Display(Name = "Landline")]
        public string Landline { get; set; }
        [Required]
        [Display(Name = "Extension")]
        public string Extention { get; set; }
        [Required]
        [Display(Name = "Locale")]
        public string Locale { get; set; }
        [Required]
        [Display(Name = "alternateemail")]
        public string alternateemail { get; set; }

    }
}
