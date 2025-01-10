using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EliteLife2024_Worker.Model
{
    public class CreateWalletHistory
    {
        public int CollaboratorId { get; set; }
        public string WalletType { get; set; }
        public decimal Value { get; set; }
        public string Note { get; set; }
    }
}
