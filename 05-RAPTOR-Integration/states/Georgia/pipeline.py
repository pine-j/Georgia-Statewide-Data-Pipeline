"""Georgia RAPTOR pipeline orchestrator.

Coordinates loading data across all six RAPTOR categories for
the Georgia statewide pipeline.
"""

import logging

from .categories.Roadways import RoadwayData

# Future category imports (Phase 2+):
# from .categories.Bridges import BridgeData
# from .categories.Safety import SafetyData
# from .categories.Traffic import TrafficData
# from .categories.Pavement import PavementData
# from .categories.Freight import FreightData

logger = logging.getLogger(__name__)


class GeorgiaPipeline:
    """Orchestrates data loading for all Georgia RAPTOR categories.

    Attributes:
        district_id: Optional GDOT district filter (1-7).
        roadways: RoadwayData instance.
    """

    def __init__(self, district_id: int | None = None):
        self.district_id = district_id
        self.roadways = RoadwayData(district_id=district_id)
        # self.bridges = BridgeData(district_id=district_id)
        # self.safety = SafetyData(district_id=district_id)
        # self.traffic = TrafficData(district_id=district_id)
        # self.pavement = PavementData(district_id=district_id)
        # self.freight = FreightData(district_id=district_id)

    def load_all(self) -> None:
        """Load data for all available categories."""
        logger.info("Loading Georgia pipeline data (district=%s)...", self.district_id)

        logger.info("Loading roadway inventory...")
        self.roadways.load_data()

        # Future phases:
        # logger.info("Loading bridge data...")
        # self.bridges.load_data()
        # logger.info("Loading safety data...")
        # self.safety.load_data()
        # logger.info("Loading traffic data...")
        # self.traffic.load_data()
        # logger.info("Loading pavement data...")
        # self.pavement.load_data()
        # logger.info("Loading freight data...")
        # self.freight.load_data()

        logger.info("Georgia pipeline data loaded.")

    def clear_all(self) -> None:
        """Release all loaded data from memory."""
        self.roadways.clear_data()
        logger.info("All Georgia pipeline data cleared.")
