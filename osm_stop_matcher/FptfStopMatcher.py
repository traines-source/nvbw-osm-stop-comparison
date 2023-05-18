import logging
import ngram

from rtree import index
from haversine import haversine, Unit
from osm_stop_matcher.util import get_parent_station

from . import config


class FptfStopMatcher():
	def __init__(self, db):
		self.db = db
		self.fptf_stops = index.Index()
		self.logger = logging.getLogger('osm_stop_matcher.FptfStopMatcher')

	def load_fptf_index(self):
		if self.fptf_stops is None:
			return
		self.logger.info("Loading fptf data to index")
		cur = self.db.execute("SELECT * FROM fptf_stops")
		cnt = 0
		rows = cur.fetchall()
		for stop in rows:
			cnt += 1
			id = stop["ibnr"]
			lat = stop["lat"]
			lon = stop["lon"]
			stop = {
				"id": id,
				"name": stop["name"],
				"network": None,
				"operator": None,
				"lat": lat,
				"lon": lon,
				"mode": stop["mode"],
				"type": None,
				"ref": stop["ifopt_id"],
				"ref_key": None,
				"ibnr": id,
				"next_stops": None,
				"prev_stops": None,
				"assumed_platform": None,
				"lines": stop["lines"]
			}
			self.fptf_stops.insert(id = cnt, coordinates=(lat, lon, lat, lon), obj=stop)
		self.logger.info("Loaded fptf data to index")

	def rank_osm_fptf_candidate(self, osm_match, fptf_match):
		o = osm_match["match"]
		f = fptf_match["match"]

		if o["ibnr"] == f["ibnr"]:
			return 1

		distance = haversine((o["lat"],o["lon"]), (f["lat"],f["lon"]), unit=Unit.METERS)

		name_distance = ngram.NGram.compare(o["name"],f["name"],N=1)
		if (o["name"] is None or f["name"] is None):
			name_distance = 0.3
		
		mode_rating = 0
		if o["mode"] == f["mode"]:
			mode_rating = 1
		elif not o["mode"] or not f["mode"]:
			mode_rating = 0.7

		rating = name_distance / ( 1 + distance / 100.0 )
		platform_rating = 0.9
		successor_rating = 0
		rating = (rating * (0.5 + 0.5 * platform_rating)) ** (1 - successor_rating * 0.3 - mode_rating * 0.2)
		return rating
		
	def get_nearest(self, coords, no_of_candidates):
	 	return list(self.fptf_stops.nearest(coords, no_of_candidates, objects='raw'))

	def lines_rating(self, stop, fptf_match):
		if fptf_match["match"]["lines"] and stop["linien"]:
			stop_lines = set(stop["linien"].split(","))
			fptf_lines = set(fptf_match["match"]["lines"].split(","))
			return len(stop_lines.intersection(fptf_lines))/len(stop_lines.union(fptf_lines))
		return 0
	
	def extended_rating(self, stop, fptf_match):
		rating = fptf_match["name_distance"] / ( 1 + fptf_match["distance"] / 10.0 )
		rating = (rating * (0.5 + 0.5 * 0)) ** (1 - self.lines_rating(stop, fptf_match) * 0.3 - fptf_match["mode_rating"] * 0.2)
		fptf_match["rating"] = rating
		if fptf_match["match"]["ref"]:
			if get_parent_station(stop["globaleID"]) == get_parent_station(fptf_match["match"]["ref"]):
				if stop["mode"] == 'train':
					fptf_match["rating"] = 1
			else:
				fptf_match["rating"] = 0

	def fptf_match_stop(self, stop, osm_matches, fptf_matches):
		for fptf_match in fptf_matches:
			self.extended_rating(stop, fptf_match)
			fptf_match["ibnr"] = fptf_match["match"]["ibnr"]
			fptf_match["fptf_rating"] = fptf_match["rating"]
			fptf_match["osm_fptf_rating"] = None
		
		if config.FPTF_ONLY_MODE:
			return fptf_matches
				
		for osm_match in osm_matches:			
			best_fptf_rating = 0
			best_osm_fptf_rating = 0
			best_rating_match = None
			for fptf_match in fptf_matches:
				rating = self.rank_osm_fptf_candidate(osm_match, fptf_match)
				if fptf_match["rating"] + rating > best_fptf_rating + best_osm_fptf_rating:
					best_fptf_rating = fptf_match["rating"]
					best_osm_fptf_rating = rating
					best_rating_match = fptf_match
			if best_rating_match is not None:
				osm_match["ibnr"] = best_rating_match["match"]["ibnr"]
				osm_match["fptf_rating"] = best_fptf_rating
				osm_match["osm_fptf_rating"] = best_osm_fptf_rating
				#osm_match["rating"] = min(1, osm_match["rating"] + 0.1*(best_fptf_rating + best_osm_fptf_rating))
			else:
				osm_match["ibnr"] = None
				osm_match["fptf_rating"] = None
				osm_match["osm_fptf_rating"] = None
		return osm_matches