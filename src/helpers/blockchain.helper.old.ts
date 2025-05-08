import Web3 from 'web3';
import { BooleanEnum, TokenStandard } from "../enum";
import { Utility_Helper } from "./utility.helper";
import { config } from '../config';
import { AbiItem } from 'web3-utils';
import { ethers } from 'ethers';

class BlockchainHelper {
    public BSC_Web3: Web3;
    public BNB_FULLNODE: string = config.config.NODE.BNB_RPC_URL;

    constructor() {
        /* BSC CASE*/
        const bsc_provider = new Web3.providers.HttpProvider(this.BNB_FULLNODE);
        this.BSC_Web3 = new Web3(bsc_provider);
    }

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
            // return 0;
            return null;
        }
    }

    public async bep20_token_balance(wallet_address: string, contract_address: string | null) {
        try {
            if (contract_address) {
                const contract: any = await new this.BSC_Web3.eth.Contract(
                    config.config.CONTRACT_ABI as AbiItem[],
                    contract_address
                );
                const decimals = await contract.methods.decimals().call();
                const balance = await contract.methods.balanceOf(wallet_address).call();
                const tokenBalance = await Utility_Helper.bigNumberSafeMath(
                    balance, '/', Math.pow(10, decimals)
                );
                let tokBalance: string = await Utility_Helper.exponentialToDecimal(tokenBalance)
                return Number(tokBalance);
            }
            return 0;
        } catch (err: any) {
            console.error(`BscHelper BEP20_Token_Fetch_Balance error >>`, err);
            // return 0;
            // return null;

            throw err;
        }
    }

    public async bnb_coin_balance(address: string) {
        try {
            const balance: string = await this.BSC_Web3.eth.getBalance(address);
            const tokenBalance = await Utility_Helper.bigNumberSafeMath(
                balance, '/', Math.pow(10, 18)
            );
            let coinBalance: string = await Utility_Helper.exponentialToDecimal(tokenBalance)
            return Number(coinBalance || 0);
        } catch (err: any) {
            console.error(`Bsc_Helper Coin_Fetch_Balance error >>`, err);
            // return 0;
            // return null;
            throw err;
        }
    }

    public async convertWeiToBsc(value: number) {
        try {
            return this.BSC_Web3.utils.fromWei(value.toString(), 'ether');
        } catch (err: any) {
            console.error(`ConvertWeiToBsc error >>`, err);
            return 0;
        }
    }

}
export let Blockchain_Helper = new BlockchainHelper();