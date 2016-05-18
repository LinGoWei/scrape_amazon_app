use app_db;

CREATE TABLE `tb_products` (
  `app_id` varchar(30) NOT NULL,
  `platform` varchar(50) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `description` text DEFAULT NULL,
  `icon_url` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`app_id`, `platform`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `tb_event` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `app_id` varchar(30) DEFAULT NULL,
  `old_name` varchar(255) DEFAULT NULL,
  `old_desc` text,
  `new_name` varchar(255) DEFAULT NULL,
  `new_desc` text,
  `date_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;
