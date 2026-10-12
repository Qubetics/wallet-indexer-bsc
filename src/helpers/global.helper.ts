import axios from "axios";
import { config } from "../config/config";
import { catchErrorMsgsModel, CoinInterface } from "../models";
import { walletModel, customTokennModel } from "../models/tables/index"
import bscProcessHelper from "../processes/process.helper";

class GlobalHelper {
    public async checkCoinInWallet(wallet_address: string, coin: CoinInterface, balance: any): Promise<boolean> {
        try {
            // Input validation
            if (!wallet_address) {
                console.error("wallet_address is required");
                return false;
            }

            if (!coin || !coin.coin_id) {
                console.error("coin with coin_id is required");
                return false;
            }

            // Check if wallet exists
            let wallet_data: any = await walletModel.findOne({
                attributes: ["balance", "user_id", "wallet_name"],
                where: {
                    wallet_address: wallet_address,
                    coin_id: coin.coin_id,
                },
                raw: true
            });

            if (!wallet_data) {
                // Get wallet data from Redis
                wallet_data = await bscProcessHelper.check_our_wallet_address(wallet_address);

                if (!wallet_data || !wallet_data.user_id) {
                    console.error(`No wallet data found for address: ${wallet_address}`);
                    return false;
                }

                // Create new wallet record
                const data_obj: any = {
                    user_id: wallet_data.user_id,
                    wallet_name: wallet_data.wallet_name,
                    wallet_address: wallet_address,
                    coin_id: coin.coin_id,
                    coin_family: coin.coin_family,
                    balance: balance !== null ? balance : 0,
                    default_wallet: 1,
                    status: 1
                };

                await walletModel.create(data_obj);
            }

            // Handle custom token creation if needed
            if (coin.added_by === 'user') {
                await this.handleCustomTokenCreation(wallet_data.user_id, coin.coin_id);
            }

            return true;
        } catch (err: any) {
            console.error("Error in checkCoinInWallet:", err);
            return false;
        }
    }

    private async handleCustomTokenCreation(user_id: number, coin_id: number): Promise<void> {
        try {
            // Check if custom token already exists
            const user_exist: any = await customTokennModel.findOne({
                attributes: ["id"],
                where: { user_id, coin_id },
                raw: true
            });

            if (!user_exist) {
                // Create custom token record
                const currentUTCDate: string = new Date()
                    .toISOString()
                    .replace(/T/, " ")
                    .replace(/\..+/, "");

                await customTokennModel.create({
                    coin_id: coin_id,
                    user_id: user_id,
                    created_at: currentUTCDate,
                    updated_at: currentUTCDate
                });
            }
        } catch (err: any) {
            console.error("Error in handleCustomTokenCreation:", err);
        }
    }

    public async sendMail(email: string, text: string, subject: string) {
        try {
            const MAILGUN_API_KEY: string = `${config.MAILGUN.MAILGUN_API_KEY}`;
            const MAILGUN_DOMAIN: string = `${config.MAILGUN.MAILGUN_DOMAIN}`;
            const MAILGUN_BASE_URL: string = `${config.MAILGUN.MAILGUN_BASE_URL}${MAILGUN_DOMAIN}`;
            await axios({
                method: 'post',
                url: `${MAILGUN_BASE_URL}/messages`,
                auth: {
                    username: 'api',
                    password: MAILGUN_API_KEY
                },
                params: {
                    from: config.MAILGUN.FROM_GMAIL,
                    to: email,
                    subject: subject,
                    html: text
                }
            });
        } catch (err: any) {
            console.error("sendMail", err)
            await this.saveErrorLogs("MAILGUN_ERROR", err.message)
        }
    }

    public async saveErrorLogs(fn_name: string, error_message: any) {
        try {
            let value_exist: any = await bscProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.BSC_ERROR_LOGS)
            if (value_exist) {
                if (Number(value_exist) == 0) {
                    await catchErrorMsgsModel.create({
                        fx_name: fn_name,
                        error_msg: error_message || {}
                    })
                    await bscProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.BSC_ERROR_LOGS, '1')
                } else {
                    console.log("value other than 0 exist")
                }
            } else {
                await bscProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.BSC_ERROR_LOGS, '0')
                console.log("value does not exist")
            }
        } catch (err: any) {
            console.error("Error in saveErrorLogs", err)
        }
    }
}
export let globalHelper = new GlobalHelper();
