-- phpMyAdmin SQL Dump
-- version 3.4.1
-- http://www.phpmyadmin.net
--
-- Host: 127.0.0.1
-- Generation Time: Feb 27, 2013 at 11:57 PM
-- Server version: 5.5.20
-- PHP Version: 5.4.12

SET SQL_MODE="NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `test`
--

-- --------------------------------------------------------

--
-- Table structure for table `documents`
--

CREATE TABLE IF NOT EXISTS `documents` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cate_ids` varchar(255) NOT NULL,
  `group_id` int(11) NOT NULL,
  `group_id2` int(11) NOT NULL,
  `date_added` datetime NOT NULL,
  `title` varchar(255) NOT NULL,
  `content` text NOT NULL,
  `latitude` float(8,6) NOT NULL,
  `longitude` float(9,6) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=6 ;

--
-- Dumping data for table `documents`
--

INSERT INTO `documents` (`id`, `cate_ids`, `group_id`, `group_id2`, `date_added`, `title`, `content`, `latitude`, `longitude`) VALUES
(1, '1', 1, 5, '2012-01-10 14:50:39', 'test one', 'this is my test document number one. also checking search within phrases.', 29.872707, 121.560455),
(2, '1,2', 1, 6, '2012-01-10 14:50:39', 'test two', 'this is my test document number two', 29.862896, 121.537651),
(3, '1,2,3', 2, 7, '2012-01-10 14:50:39', 'another doc', 'this is another group', 29.872707, 121.560455),
(4, '1,2,3,4', 2, 8, '2012-01-10 14:50:39', 'doc number four', 'this is to test groups', 29.980810, 121.764984),
(5, '', 0, 0, '0000-00-00 00:00:00', 'this is for document field weights', 'For field weights test.', 31.321966, 121.915077);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
