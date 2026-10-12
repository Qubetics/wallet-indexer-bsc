import { BooleanEnum, TokenStandard } from "../enum";
import { Utility_Helper } from "./utility.helper";
import { config } from '../config';
import { ethers } from 'ethers';

class BlockchainHelper {
    public provider: ethers.JsonRpcProvider;
  
    constructor() {
      this.provider = new ethers.JsonRpcProvider(config.config.NODE.BNB_RPC_URL || "");
      this.initialize();
    }

    public initialize() {}
  
    //BNB
    public async fetch_bnb_balance(address: string, coin: any) {
        try {
            let balance: number | null = 0;
            if (coin.is_token == BooleanEnum.true) {
                switch (coin.token_type.toLowerCase()) {
                    case TokenStandard.BEP20:
                        balance = await this.bep20_token_balance(
                            address,
                            coin.token_address ? coin.token_address : null
                        )
                        break;
                    default:
                        balance = 0
                        break;
                }
            } else {
                balance = await this.bnb_coin_balance(address);
            }
            return balance
        } catch (err: any) {
            console.error(`Bsc_Helper BSC_Fetch_Balance error >>`, err);
            return null;
        }
    }

    public async bep20_token_balance(wallet_address: string, contract_address: string | null) {
        try {
            if (contract_address) {
                const contract = new ethers.Contract(
                  contract_address,
                  config.config.CONTRACT_ABI,
                  this.provider
                );
                const decimals = Number(await contract.decimals())
                const balance = await contract.balanceOf(wallet_address);
                const tokenBalance = await Utility_Helper.bigNumberSafeMath(
                    balance, '/', Math.pow(10, decimals)
                );
                const tokBalance: string = await Utility_Helper.exponentialToDecimal(tokenBalance)
                return Number(tokBalance);
              }
            return 0;
        } catch (err: any) {
            console.error(`BscHelper BEP20_Token_Fetch_Balance error >>`, err);

            throw err;
        }
    }

    public async bnb_coin_balance(address: string) {
        try {
            const balance = await this.provider.getBalance(address);
            const tokenBalance = await Utility_Helper.bigNumberSafeMath(
                balance, '/', Math.pow(10, 18)
            );
            const coinBalance: string = await Utility_Helper.exponentialToDecimal(tokenBalance)
            return Number(coinBalance || 0);
        } catch (err: any) {
            console.error(`Bsc_Helper Coin_Fetch_Balance error >>`, err);
            throw err;
        }
    }

    public async convertWeiToBsc(value: number | string) {
        try {
            const valueAsString = value.toString();
            return ethers.formatEther(valueAsString);;
        } catch (err: any) {
            console.error(`ConvertWeiToBsc error >>`, err);
            return 0;
        }
    }

}
export let Blockchain_Helper = new BlockchainHelper();