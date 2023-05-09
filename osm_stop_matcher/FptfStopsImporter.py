import logging
from osm_stop_matcher.util import drop_table_if_exists
import json

class FptfStopsImporter():

	def __init__(self, db):
		self.logger = logging.getLogger('osm_stop_matcher.FptfStopsImporter')
		self.db = db
		
	def setup_fptf_tables(self):
		drop_table_if_exists(self.db, 'fptf_stops')
		self.db.execute('''CREATE TABLE fptf_stops
			(ibnr TEXT PRIMARY KEY, name TEXT, lat REAL, lon REAL, mode TEXT)''')

	def import_stops(self, stops_file):
		self.setup_fptf_tables()
		cur = self.db.cursor()
		counter = 0
		with open(stops_file) as f:
			rows_to_import = []
			for l in f.readlines():
				stop = json.loads(l)

				if stop["location"] is None:
					continue

				rows_to_import.append((
					stop["id"], 
					stop["name"],
					stop["location"]["latitude"],
					stop["location"]["longitude"],
					self.extract_stop_mode(stop),
				))
				counter += 1
				
				if counter % 1000 == 0:
					cur.executemany('INSERT INTO fptf_stops VALUES (?,?,?,?,?)', rows_to_import)
					rows_to_import = []
					if counter % 10000 == 0:
						self.logger.info("Imported %s stops", counter)
				
					
		self.logger.info("Loaded stops from FPTF file")
		cur.execute("""ALTER TABLE fptf_stops ADD COLUMN match_state TEXT""")
		self.db.commit()
		self.logger.info("Added match state")



	def extract_stop_mode(self, stop):
		if "products" not in stop:
			return None
		tags = stop["products"]
		
		ordered_ref_keys = ["bus", "nationalExpress", "regionalExpress", "national", "regional", "suburban", "subway", "tram", "ferry"]
		first_occurrence = None
		for key in ordered_ref_keys:
			if key in tags and tags[key]:
				if first_occurrence:
					# if ambigous, rather return nothing than wrong
					if first_occurrence == 'bus':
						return None
					else:
						return 'trainish'
				else:
					first_occurrence = key
		if first_occurrence == 'bus' or first_occurrence == 'ferry':
			return first_occurrence
		elif first_occurrence == 'tram':
			# HAFAS tram might be tram or light_rail (e.g. Stuttgart Stadtbahn), impossible to distinguish
			return None
		elif first_occurrence is not None:
			return 'trainish'
		return None