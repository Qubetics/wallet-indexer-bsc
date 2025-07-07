import { DataTypes, Model, Optional } from "sequelize";
import { BscOldBlockInterface } from "../interfaces/index";
import db from "../../config/db";

interface BscOldBlockCreationModel extends Optional<BscOldBlockInterface, "id"> { }
interface BscOldBlockInstance
    extends Model<BscOldBlockInterface, BscOldBlockCreationModel>,
    BscOldBlockInterface { }

let dataObj = {
    id: {
        primaryKey: true,
        type: DataTypes.INTEGER,
        allowNull: false,
        autoIncrement: true,
    },
    block_number: {
        type: DataTypes.INTEGER,
        allowNull: false,
    },
    start_block: {
        type: DataTypes.INTEGER,
        allowNull: true,
    },
    end_block: {
        type: DataTypes.INTEGER,
        allowNull: true,
    },
    coin_family: {
        type: DataTypes.INTEGER,
        allowNull: false,
    },
    status: {
        type: DataTypes.TINYINT,
        allowNull: true
    }
};
let dataObjIndex = {
    createdAt: 'created_at',
    updatedAt: 'updated_at',
    indexes: [
        {
            unique: false,
            fields: ["status"],
        }
    ]
}


const BscOldBLockModel = db.db_write.define<BscOldBlockInstance>("bsc_old_blocks", dataObj, dataObjIndex);

export default BscOldBLockModel;