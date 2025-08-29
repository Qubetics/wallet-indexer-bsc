import { config } from "../config";
import kafkaHelper from "../config/kafka.helper";
import { CoinFamilyEnum } from "../enum";
import { CoinInterface, CoinsModel } from "../models";
import WalletModel from "../models/tables/model.wallets";
import { blockchainHelper } from "./blockchain.helper";
import { utilityHelper } from "./utility.helper";
class WalletHelper {
    public bsc_pending_balance_update_topic = config.config.KAFKA_BALANCE.BSC_PENDING_BALANCE_UPDATE_TOPIC;

    public async fetchBalance(address: string, coin: CoinInterface,) {
        try {
            let balance: number | null = null;
            if (coin) {
                switch (coin.coin_family) {
                    case CoinFamilyEnum.BNB:
                        balance = await blockchainHelper.fetchBnbBalance(address, coin);
                        break;
                    default:
                        balance = 0;
                        break;
                }
            }
            return balance !== null ? Number(await utilityHelper.exponentialToDecimal(balance)) : null;
        } catch (err: any) {
            console.error(`error WalletHelper fetchBalance error >>>`, err);
            return null;
        }
    }


    public async updateBalance(wallet_address: string, coin: CoinInterface) {
        try {
            if (!wallet_address) {
                console.error(`wallet_address missing >>> `);
                return false;
            }
            /* fatch wallet balnce */
            let balance: number | null = await this.fetchBalance(
                wallet_address,
                coin
            );
            if (balance !== null) {
                let wallet_data: any = await WalletModel.findOne({
                    attributes: ["balance"],
                    where: {
                        wallet_address: wallet_address,
                        coin_id: coin.coin_id,
                    }
                })
                if (wallet_data) {
                    await WalletModel.update({
                        balance: balance,
                        status: 1
                    }, {
                        where: {
                            wallet_address: wallet_address,
                            coin_id: coin.coin_id,
                        }
                    });
                }
            }
            else {
                let data = {
                    coin_data: coin,
                    wallet_address: wallet_address,
                    queue_count: 0,
                }
                await kafkaHelper.produceMessage(this.bsc_pending_balance_update_topic, data)
            }
            return balance;
        } catch (err: any) {
            console.error(`WalletHelper updateBalance error >>>`, err);
            return null;
        }
    }

    public async updateAllActiveCoinBalance(wallet_address: string, coin_family: CoinFamilyEnum) {
        try {
            let address_coins: any = await WalletModel.findAll({
                attributes: ["coin_id", "wallet_address"],
                where: {
                    wallet_address: wallet_address,
                    status: 1,
                    coin_family: coin_family
                },
                include: [{
                    model: CoinsModel,
                    attributes: ["coin_id", "coin_name", "coin_symbol", "coin_family", "is_token", "token_type", "token_address"],
                    as: "coin_data",
                    required: true,
                    where: {
                        coin_status: 1,
                    },
                }],
            })
            for (let i = 0; i < address_coins.length; i++) {
                await this.updateBalance(address_coins[i].wallet_address, address_coins[i].coin_data);
            }
            return 1;
        } catch (err: any) {
            console.error(`WalletHelper updateAllActiveCoinBalance error >>>`, err);
            return 0;

        }
    }


    // update balance queue


    public async updateBalanceQueue(wallet_address: string, coin: CoinInterface) {
        try {
            if (!wallet_address) {
                console.error(`wallet_address missing >>> `);
                return false;
            }
            /* fatch wallet balnce */
            const balance: number | null = await this.fetchBalance(
                wallet_address,
                coin
            );
            return balance;
        } catch (err: any) {
            console.error(`WalletHelper updateBalanceQueue error >>>`, err);
            return null;
        }
    }
}
export let walletHelper = new WalletHelper();
