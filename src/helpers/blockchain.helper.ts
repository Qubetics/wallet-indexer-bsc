import Web3 from 'web3';
import TronWeb from 'tronweb';
import { BooleanEnum, TokenStandard } from "../enum";
import { Utility_Helper } from "./utility.helper";
import { config } from '../config';
import { AbiItem } from 'web3-utils';

class BlockchainHelper {
    // public REDIS_TOKENS: any = CONFIG.REDISKEYS.TOKENS;
    public BSC_Web3: Web3;
    public MATIC_Web3: Web3;
    public ETH_Web3: Web3;
    public tronWeb: any;

    public BNB_FULLNODE: string = config.config.NODE.BNB_RPC_URL;
    public TRX_FULLNODE: string = config.config.NODE.TRX_RPC_URL;
    public ETH_FULLNODE: string = config.config.NODE.ETH_RPC_URL;

    constructor() {
        /* TRON CASE */
        const HttpProvider = TronWeb.providers.HttpProvider;
        const fullNode = new HttpProvider(this.TRX_FULLNODE);
        // this.tronWeb = new TronWeb({ fullHost: this.TRX_FULLNODE }); // Old used
        this.tronWeb = new TronWeb({
            fullHost: this.TRX_FULLNODE,
            headers: { apikey: config.config.NODE.TRX_API_KEY }
        });
        /* BSC CASE*/
        const bsc_provider = new Web3.providers.HttpProvider(this.BNB_FULLNODE);
        this.BSC_Web3 = new Web3(bsc_provider);
        /* ETH CASE*/
        const eth_provider = new Web3.providers.HttpProvider(this.ETH_FULLNODE);
        this.ETH_Web3 = new Web3(eth_provider);
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
    //TRX
    public async TRX_Fetch_Balance(address: string, coin: any) {
        try {
            let balance: number = 0;
            if (coin.is_token == BooleanEnum.true) {
                switch (coin.token_type.toLowerCase()) {
                    case TokenStandard.ERC20:
                        balance = await this.TRC20_Token_Balance(
                            address,
                            coin.token_address
                        )
                        break;
                    default:
                        balance = 0
                        break;
                }
            } else {
                balance = await this.TRX_Coin_Fetch_Balance(address);
            }
            return balance
        } catch (err: any) {
            console.error(`TRON_Helper TRON_Fetch_Balance error >>`, err);
           // return 0;
           return null;
        }
    }
    public async TRC20_Token_Balance(address: string, contract_address: string) {
        try {
            await this.tronWeb.setAddress(contract_address);
            let contract = await this.tronWeb.contract().at(contract_address);
            let decimals = await contract.decimals().call();
            if (decimals._hex != undefined) {
                decimals = await this.tronWeb.toDecimal(decimals._hex);
            }
            let balanceOf = await contract.balanceOf(address).call();
            let toDecimal = await this.tronWeb.toDecimal(balanceOf._hex);
            if (balanceOf._hex == undefined) {
                toDecimal = await this.tronWeb.toDecimal(
                    balanceOf.balance._hex
                );
            }
            let expToDecimal = await Utility_Helper.exponentialToDecimal(toDecimal);
            let balance = Number(expToDecimal) / Math.pow(10, decimals);
            return balance;
        } catch (error: any) {
            console.error(`Tron_Helper TRC20_Token_Balance error >>`, error);
            //return 0;
            throw error;
        }
    }
    public async TRX_Coin_Fetch_Balance(address: string) {
        try {
            return await this.tronWeb.trx.getBalance(address).then(async (result: any) => {
                return await this.tronWeb.fromSun(result);
            });
        } catch (error: any) {
            console.error(`Tron_Helper TRX_Fetch_Balance error >>`, error);
           // return 0;
           throw error;
        }
    }
    //ETH
    public async ETH_Fetch_Balance(address: string, coin: any) {
        try {
            let balance: number | null = 0;
            if (coin.is_token == 1) {
                switch (coin.token_type.toLowerCase()) {
                    case TokenStandard.ERC20:
                        balance = await this.ERC20_Token_Fetch_Eth_Balance(
                            address,
                            coin.token_address ? coin.token_address : null
                        )
                        break;
                    default:
                        balance = 0
                        break;
                }
            } else {
                balance = await this.ETH_Coin_Fetch_Balance(address);
            }
            return balance
        } catch (error: any) {
            console.error(`ETH_Helper ETH_Fetch_Balance error >>`, error);
            return null;
            // return 0;
        }
    }
    public async ERC20_Token_Fetch_Eth_Balance(wallet_address: string, contract_address: string | null) {
        try {
            if (contract_address) {
                const contract: any = await new this.ETH_Web3.eth.Contract(
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
            console.error(`ETHHelper ERC20_Token_Fetch_Balance error >>`, err);
            // return 0;
           // return null;
           throw err;
        }
    }
    public async ETH_Coin_Fetch_Balance(address: string) {
        try {
            const balance: any = await this.ETH_Web3.eth.getBalance(address);
            const tokenBalance = await Utility_Helper.bigNumberSafeMath(
                balance, '/', Math.pow(10, 18)
            );
            let coinBalance: string = await Utility_Helper.exponentialToDecimal(tokenBalance)
            return Number(coinBalance || 0);
        } catch (err: any) {
            console.error(`ETH_Helper Coin_Fetch_Balance error >>`, err);
            //return 0;

            throw err;
        }
    }




}
export let Blockchain_Helper = new BlockchainHelper();