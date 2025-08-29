import axios from "axios";
import { config } from "../config/config";
import { CatchErrorMsgsModel, CoinInterface } from "../models";
import { WalletModel, CustomTokennModel, CoinsModel } from "../models/tables/index"
import bscProcessHelper from "../processes/process.helper";
import redisClient from "../config/redis";

class Global_helper {
    public async check_coin_in_wallet(wallet_address: string, coin: CoinInterface, balance: any) {
        try {
            let wallet_data: any = await WalletModel.findOne({
                attributes: ["balance", "user_id", "wallet_name"],
                where: {
                    wallet_address: wallet_address,
                    coin_id: coin.coin_id,
                },
                raw: true
            })
            if (!wallet_data) {
                wallet_data = await bscProcessHelper.check_our_wallet_address(wallet_address)
                let data_obj: any = {
                    user_id: wallet_data.user_id,
                    wallet_name: wallet_data.wallet_name,
                    wallet_address: wallet_address,
                    coin_id: coin.coin_id,
                    coin_family: coin.coin_family,
                    balance: balance !== null ? balance : 0,
                    default_wallet: 1,
                    status: 1
                }
                await WalletModel.create(data_obj);
            }

            console.log("wallet created>>", wallet_data)
            if (coin.added_by) {
                console.log("coin.added_by exist", coin.added_by)
                if (coin.added_by == 'user') {
                    console.log("added by user")
                    let user_exist: any = await CustomTokennModel.findOne({
                        attributes: ["id"],
                        where: { user_id: wallet_data.user_id, coin_id: coin.coin_id },
                        raw: true
                    })
                    if (user_exist) {
                        console.log("custom token already exist")
                    } else {
                        console.log("custom token not exist")
                        let currentUTCDate: string = new Date()
                            .toISOString()
                            .replace(/T/, " ")
                            .replace(/\..+/, "");
                        await CustomTokennModel.create({
                            coin_id: coin.coin_id,
                            user_id: wallet_data.user_id,
                            created_at: currentUTCDate,
                            updated_at: currentUTCDate
                        })
                    }

                }
            } else {
                console.log("coin.added_by does not exist")
            }
        } catch (err: any) {
            console.error("Error in check_coin_in_wallet>>>", err)
            return err;
        }

    }
    public async send_mail(email: string, text: string, subject: string) {
        try {
            const MAILGUN_API_KEY: string = `${config.MAILGUN.MAILGUN_API_KEY}`;
            const MAILGUN_DOMAIN: string = `${config.MAILGUN.MAILGUN_DOMAIN}`;
            const MAILGUN_BASE_URL: string = `${config.MAILGUN.MAILGUN_BASE_URL}${MAILGUN_DOMAIN}`;
            const response: any = await axios({
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
            console.error("send_mail", err)
            await this.save_error_logs("MAILGUN_ERROR", err.message)
        }
    }
    public async save_error_logs(fn_name: string, error_message: any) {
        try {
            let value_exist: any = await bscProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.BSC_ERROR_LOGS)
            if (value_exist) {
                if (Number(value_exist) == 0) {
                    await CatchErrorMsgsModel.create({
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
            console.error("Error in save_error_logs", err)
        }
    }


    public async coinCreate(obj: any) {
        try {
            const data: any = await CoinsModel.create(obj);
            return data;
        } catch (err: any) {
            console.error("Error in coin_create>>", err);

            const uniqueConstraint =
                err.name === 'SequelizeUniqueConstraintError' &&
                err.errors?.some(
                    (e: any) => e.path === 'unique_coinfamily_tokenaddress' || e.message?.includes('unique_coinfamily_tokenaddress')
                );

            if (uniqueConstraint) {
                const tokenAddress = obj.token_address;
                const coinFamily = obj.coin_family;

                try {
                    const existing = await CoinsModel.findOne({
                        where: {
                            token_address: tokenAddress,
                            coin_family: coinFamily,
                        },
                        raw: true
                    });
                    console.log("🚀 ~ CoinQueries ~ coin_create ~ existing:", existing)

                    if (existing) {
                        return existing;
                    }
                } catch (findErr) {
                    console.error("Error finding existing coin>>", findErr);
                    await this.save_error_logs("coin_create_find_existing", (findErr as any).message);
                }
            }

            await this.save_error_logs("coin_create", err.message);
            return null;
        }
    }

    public async setHashTable(key: string, field: any, value: any, expiryInSeconds?: number) {
        try {
            await redisClient.HSET(key, field.toUpperCase(), value, (err: any, reply: any) => {
                if (err) {
                    console.error('redis set string data error...', err)
                }
            });
            if (expiryInSeconds) {
                await redisClient.EXPIRE(key, expiryInSeconds);
            }
        } catch (err: any) {
            console.error('💥 ~ ~ set_hash_table error');
            throw err;
        }
    }

    public async updateTokenInRedisHash(coin_data: any) {
        try {
            await this.setHashTable(
                config.TOKEN_TYPE_BSC,
                coin_data.token_address,
                JSON.stringify(coin_data)
            );

        } catch (err: any) {
            console.error("Error in update_token_in_redis_hash.", err);
            await this.save_error_logs(
                "update_token_in_redis_hash",
                err.message
            );
            return null;
        }

    }

    public async customTokenCreate(obj: any) {
        try {
            let data: any = await CustomTokennModel.create(obj)
            return data;
        } catch (err: any) {
            console.error("Error in custom_token_create>>", err);
            await this.save_error_logs("custom_token_create", err.message);
            return null;
        }
    }
}
export let global_helper = new Global_helper();
