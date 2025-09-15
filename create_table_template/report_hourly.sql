CREATE TABLE `report_hourly` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `timestamp` int(11) NOT NULL COMMENT '时间戳，当前小时的0分',
  `media_id` varchar(31) NOT NULL COMMENT '媒体id',
  `adspot_id` varchar(31) NOT NULL COMMENT '广告位id',
  `sdk_adspot_id` varchar(100) NOT NULL DEFAULT '-' COMMENT 'Sdk渠道的广告位id',
  `channel_id` varchar(31) NOT NULL COMMENT 'Sdk渠道的渠道id',
  `pvs` bigint(20) NOT NULL DEFAULT '0' COMMENT '广告位请求数',
  `reqs` bigint(20) NOT NULL DEFAULT '0' COMMENT '渠道广告请求数',
  `bids` bigint(20) NOT NULL DEFAULT '0' COMMENT '渠道广告返回数',
  `wins` int(11) NOT NULL DEFAULT '0' COMMENT '渠道广告胜出数',
  `shows` int(11) NOT NULL DEFAULT '0' COMMENT '渠道广告展现数',
  `clicks` int(11) NOT NULL DEFAULT '0' COMMENT '渠道广告点击数',
  `income` float NOT NULL DEFAULT '0' COMMENT '收入',
  PRIMARY KEY (`id`) USING HASH,
  UNIQUE KEY `uniKey` (`timestamp`,`media_id`,`adspot_id`,`sdk_adspot_id`,`channel_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8