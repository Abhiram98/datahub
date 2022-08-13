from typing import List


import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetTransformer,
)
from datahub.metadata.schema_classes import BrowsePathsClass, MetadataChangeEventClass
import logging

logger = logging.getLogger(__name__)

class SetDatabaseNameAliasConfig(ConfigModel):
    database_name: str
    alias_name: str


class SetDatabaseNameAliasTransformer(DatasetTransformer):
    """Transformer that can be used to set browse paths through template replacement"""

    ctx: PipelineContext
    config: SetDatabaseNameAliasConfig

    def __init__(self, config: SetDatabaseNameAliasConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SetDatabaseNameAliasTransformer":
        config = SetDatabaseNameAliasConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        iter_urns = [mce] #TODO: replace with urn iteration fn.
        for m in iter_urns:
            platform_part, dataset, env = (
                mce.proposedSnapshot.urn.replace("urn:li:dataset:(", "")
                .replace(")", "")
                .split(",")
            )
            dataset_parts = dataset.split('.')
            logger.info(mce.proposedSnapshot.urn)
            logger.info(platform_part)
            logger.info("dataset_parts: "+str(dataset_parts))
            logger.info(env)
            db_name = dataset_parts[0]
            if db_name == self.config.database_name:
                logger.info("Transforming dataset!")
                new_urn = f"urn:li:dataset:({platform_part},{self.config.alias_name}.{'.'.join(dataset_parts[1:])},{env})"
                
                #TODO: Figure out how to replace the dataset, 
                #          instead of creating a duplicate
                m.proposedSnapshot.urn = new_urn

            else:
                logger.info("NOT Transforming dataset.")
                logger.info(f"db_name {db_name}")
                logger.info(f"self.config.database_name {self.config.database_name}")


        return mce
