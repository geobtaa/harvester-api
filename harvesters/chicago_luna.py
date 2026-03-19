import json
import hashlib
import os
import re
import time
from urllib.parse import urlparse

import pandas as pd
import requests

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table


class ChicagoLunaHarvester(BaseHarvester):
    """
    Harvester for a University of Chicago LUNA IIIF collection.

    This currently maps collection item references into a minimal schema-compatible
    table. The upstream endpoint may return an access verification page instead of
    IIIF JSON, so this class handles that case by returning an empty collection.
    """

    def __init__(self, config):
        super().__init__(config)
        self.collection_url = self.config["collection_url"]
        self.timeout = self.config.get("timeout", 60)
        self.raw_canvas_df = pd.DataFrame()

    def fetch(self):
        root_json = self._get_json(self.collection_url)
        if root_json is None:
            return {"id": self.collection_url, "label": {"en": ["Chicago Luna"]}, "items": []}

        manifest_refs = self.collect_manifest_links(root_json=root_json)
        print(f"[Chicago Luna] Collected {len(manifest_refs)} manifest links.")

        return {
            "id": root_json.get("@id") or root_json.get("id") or self.collection_url,
            "label": root_json.get("label") or self.config.get("name", "Chicago Luna"),
            "attribution": root_json.get("attribution", ""),
            "items": manifest_refs,
        }

    def collect_manifest_links(self, root_json=None):
        """
        Walk a paged IIIF collection and return one manifest reference per map.

        This supports the common IIIF Presentation 2 shape used by LUNA:
        a collection root with `first`, then one or more collection pages that
        expose `manifests` and optionally `next`.
        """
        if root_json is None:
            root_json = self._get_json(self.collection_url)
            if root_json is None:
                return []

        manifest_refs = []
        seen_manifest_urls = set()
        seen_page_urls = set()
        queued_pages = []

        root_page_manifests = self._extract_manifest_refs(root_json)
        if root_page_manifests:
            queued_pages.append((self.collection_url, root_json))
        else:
            first_page_url = self._extract_resource_id(root_json.get("first")) or self.collection_url
            queued_pages.append((first_page_url, None))

        while queued_pages:
            page_url, prefetched_json = queued_pages.pop(0)
            if not page_url or page_url in seen_page_urls:
                continue

            seen_page_urls.add(page_url)
            page_json = prefetched_json or self._get_json(page_url)
            if page_json is None:
                continue

            page_manifests = self._extract_manifest_refs(page_json)
            for manifest_ref in page_manifests:
                manifest_url = manifest_ref["id"]
                if manifest_url and manifest_url not in seen_manifest_urls:
                    seen_manifest_urls.add(manifest_url)
                    manifest_refs.append(manifest_ref)

            print(
                f"[Chicago Luna] Page {page_url} yielded {len(page_manifests)} manifests "
                f"({len(manifest_refs)} total)."
            )

            next_page_url = self._extract_resource_id(page_json.get("next"))
            if not next_page_url:
                next_page_url = self._infer_next_page_url(
                    current_page_url=page_url,
                    page_json=page_json,
                    root_json=root_json,
                    page_manifest_count=len(page_manifests),
                )

            if next_page_url and next_page_url not in seen_page_urls:
                queued_pages.append((next_page_url, None))

        return manifest_refs

    def parse(self, raw_data):
        if not isinstance(raw_data, dict):
            return []

        collection_title = self._extract_label(raw_data.get("label")) or self.config.get("name", "Chicago Luna")
        parsed_items = []

        for item in raw_data.get("items", []):
            manifest_url = self._extract_resource_id(item)
            if not manifest_url:
                continue

            try:
                manifest_json = self._get_json(manifest_url)
            except requests.exceptions.RequestException as exc:
                print(f"[Chicago Luna] Failed to fetch manifest {manifest_url}: {exc}")
                continue
            except ValueError as exc:
                print(f"[Chicago Luna] Failed to parse manifest JSON {manifest_url}: {exc}")
                continue

            if manifest_json is None:
                continue

            canvas_records = self.extract_canvas_records(
                manifest_json=manifest_json,
                collection_title=collection_title,
            )
            parsed_items.extend(canvas_records)
            print(
                f"[Chicago Luna] Manifest {manifest_url} yielded {len(canvas_records)} canvas records."
            )

        print(f"[Chicago Luna] Parsed {len(parsed_items)} collection items.")
        return parsed_items

    def extract_canvas_records(self, manifest_json, collection_title=""):
        """
        Return one record per canvas, preserving the full canvas payload plus
        flattened metadata fields that are convenient for downstream mapping.
        """
        manifest_id = self._extract_resource_id(manifest_json)
        manifest_label = self._extract_label(manifest_json.get("label"))
        manifest_related = self._extract_first_url(manifest_json.get("related"))
        manifest_service = self._extract_first_url(manifest_json.get("service"))
        manifest_attribution = self._extract_label(manifest_json.get("attribution"))

        records = []
        for sequence_index, sequence in enumerate(manifest_json.get("sequences", [])):
            if not isinstance(sequence, dict):
                continue

            for canvas_index, canvas in enumerate(sequence.get("canvases", [])):
                if not isinstance(canvas, dict):
                    continue

                metadata_map = self._metadata_to_multimap(canvas.get("metadata", []))
                image_resource = self._extract_first_image_resource(canvas.get("images", []))
                image_service = self._extract_first_url(image_resource.get("service"))
                image_url = self._extract_resource_id(image_resource) or self._extract_thumbnail(canvas.get("thumbnail"))

                canvas_descriptions = metadata_map.get("description", [])
                canvas_record = {
                    "collection_title": collection_title,
                    "manifest_id": manifest_id,
                    "manifest_label": manifest_label,
                    "manifest_attribution": manifest_attribution,
                    "manifest_related": manifest_related,
                    "manifest_service": manifest_service,
                    "manifest_json": json.dumps(manifest_json, ensure_ascii=True),
                    "canvas_index": canvas_index,
                    "sequence_index": sequence_index,
                    "canvas_id": self._extract_resource_id(canvas),
                    "canvas_type": canvas.get("@type") or canvas.get("type", ""),
                    "canvas_label": self._extract_label(canvas.get("label")),
                    "canvas_description": self._extract_label(canvas.get("description")),
                    "canvas_metadata_json": json.dumps(canvas.get("metadata", []), ensure_ascii=True),
                    "canvas_images_json": json.dumps(canvas.get("images", []), ensure_ascii=True),
                    "canvas_json": json.dumps(canvas, ensure_ascii=True),
                    "canvas_thumbnail": self._extract_thumbnail(canvas.get("thumbnail")),
                    "canvas_width": canvas.get("width", ""),
                    "canvas_height": canvas.get("height", ""),
                    "call_number": "|".join(metadata_map.get("call number", [])),
                    "filename": "|".join(metadata_map.get("filename", [])),
                    "identifier_text": "|".join(metadata_map.get("identifier", [])),
                    "title_text": "|".join(metadata_map.get("title", [])),
                    "creator": "|".join(metadata_map.get("creator", [])),
                    "subject": "|".join(metadata_map.get("subject", [])),
                    "description_text": "|".join(canvas_descriptions),
                    "publisher_text": "|".join(metadata_map.get("publisher", [])),
                    "date_text": "|".join(metadata_map.get("date", [])),
                    "type_text": "|".join(metadata_map.get("type", [])),
                    "format_text": "|".join(metadata_map.get("format", [])),
                    "coverage_text": "|".join(metadata_map.get("coverage", [])),
                    "source_id": manifest_id,
                    "source_title": self._extract_label(canvas.get("label")) or manifest_label,
                    "source_summary": self._extract_label(canvas.get("description")) or "|".join(canvas_descriptions),
                    "manifest": manifest_id,
                    "iiif": image_service,
                    "image": image_url,
                    "thumbnail": self._extract_thumbnail(canvas.get("thumbnail")),
                    "information": manifest_related or manifest_id or self.collection_url,
                }
                records.append(canvas_record)

        return records

    def build_dataframe(self, parsed_data):
        required_columns = [
            "ID",
            "Identifier",
            "Title",
            "Description",
            "Access Rights",
            "Resource Class",
            "Format",
            "Language",
            "Publisher",
            "Provider",
            "Bounding Box",
            "manifest",
            "iiif",
            "image",
            "thumbnail",
            "information",
        ]

        self.raw_canvas_df = pd.DataFrame(parsed_data)

        if not parsed_data:
            return pd.DataFrame(columns=required_columns)

        df = self.raw_canvas_df.copy()
        schema_df = pd.DataFrame()
        schema_df["ID"] = self._series_or_default(df, "canvas_id").replace("", pd.NA)
        schema_df["ID"] = schema_df["ID"].fillna(self._series_or_default(df, "manifest_id"))
        schema_df["ID"] = schema_df["ID"].apply(self._build_record_id)
        schema_df["Identifier"] = self._series_or_default(df, "identifier_text").replace("", pd.NA)
        schema_df["Identifier"] = schema_df["Identifier"].fillna(self._series_or_default(df, "manifest_id"))
        schema_df["Title"] = self._series_or_default(df, "title_text").replace("", pd.NA)
        schema_df["Title"] = schema_df["Title"].fillna(self._series_or_default(df, "canvas_label").replace("", pd.NA))
        schema_df["Title"] = schema_df["Title"].fillna(schema_df["ID"])
        schema_df["Description"] = self._series_or_default(df, "description_text").replace("", pd.NA)
        schema_df["Description"] = schema_df["Description"].fillna(
            self._series_or_default(df, "canvas_description")
        )
        schema_df["Access Rights"] = "Public"
        schema_df["Resource Class"] = "Imagery"
        schema_df["Format"] = self._series_or_default(df, "format_text").replace("", pd.NA)
        schema_df["Format"] = schema_df["Format"].fillna("Image")
        schema_df["Language"] = "eng"
        schema_df["Publisher"] = self._series_or_default(df, "publisher_text").replace("", pd.NA)
        schema_df["Publisher"] = schema_df["Publisher"]
        schema_df["Provider"] = "University of Chicago"
        schema_df["Bounding Box"] = ""
        schema_df["manifest"] = self._series_or_default(df, "manifest_id")
        schema_df["iiif"] = self._series_or_default(df, "iiif")
        schema_df["image"] = self._series_or_default(df, "image")
        schema_df["thumbnail"] = self._series_or_default(df, "thumbnail")
        schema_df["information"] = self._series_or_default(df, "information", self.collection_url)

        return schema_df.reindex(columns=required_columns)

    def add_defaults(self, df):
        df = super().add_defaults(df)
        df["Member Of"] = df.get("Member Of", "64bd8c4c-8e60-4956-b43d-bdc3f93db488")
        df["Is Part Of"] = df.get("Is Part Of", "")
        return df

    def add_provenance(self, df):
        df = super().add_provenance(df)

        today = time.strftime("%Y-%m-%d")
        df["Website Platform"] = "LUNA"
        df["Accrual Method"] = "Automated retrieval"
        df["Harvest Workflow"] = "py_chicago_luna"
        df["Endpoint Description"] = "IIIF Presentation API"
        df["Endpoint URL"] = self.collection_url
        df["Provenance"] = (
            f"The metadata for this resource was last retrieved from the University of Chicago Libraries on {today}."
        )
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)
        results = super().write_outputs(primary_df, distributions_df)

        raw_out = self.config.get("output_raw_csv")
        if raw_out:
            today = time.strftime("%Y-%m-%d")
            output_dir = "outputs"
            os.makedirs(output_dir, exist_ok=True)

            raw_filename = os.path.join(output_dir, f"{today}_{os.path.basename(raw_out)}")
            raw_df = self.raw_canvas_df.copy()
            raw_df.to_csv(raw_filename, index=False, encoding="utf-8")
            results["raw_csv"] = raw_filename

        return results

    def _get_json(self, url):
        print(f"[Chicago Luna] Fetching {url}")
        response = requests.get(url, timeout=self.timeout)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "")
        if "html" in content_type.lower() or "Verify Access" in response.text[:500]:
            print(
                "[Chicago Luna] The LUNA endpoint returned an access verification page "
                f"for {url}. Skipping this response."
            )
            return None

        return response.json()

    def _extract_manifest_refs(self, page_json):
        manifest_refs = []
        manifest_entries = []

        for key in ("manifests", "members", "items"):
            candidate = page_json.get(key, [])
            if isinstance(candidate, list):
                manifest_entries.extend(candidate)

        for entry in manifest_entries:
            manifest_url = self._extract_resource_id(entry)
            if not manifest_url:
                continue

            entry_type = ""
            if isinstance(entry, dict):
                entry_type = entry.get("@type") or entry.get("type") or ""

            if entry_type and "manifest" not in str(entry_type).lower():
                continue

            manifest_refs.append(
                {
                    "id": manifest_url,
                    "type": entry_type or "sc:Manifest",
                    "label": self._extract_label(entry.get("label")) if isinstance(entry, dict) else "",
                }
            )

        return manifest_refs

    def _infer_next_page_url(self, current_page_url, page_json, root_json, page_manifest_count):
        try:
            current_start = int(page_json.get("startIndex", 0))
            total = int(page_json.get("total") or root_json.get("total") or 0)
        except (TypeError, ValueError):
            return ""

        page_size = page_manifest_count or int(self.config.get("page_size", 0) or 0)
        next_start = current_start + page_size
        if page_size <= 0 or next_start >= total:
            return ""

        collection_base_url = (
            self._extract_resource_id(page_json.get("within"))
            or self._extract_resource_id(root_json.get("within"))
            or self._extract_resource_id(root_json)
            or self.collection_url
        )

        collection_base_url = collection_base_url.rstrip("/")
        return f"{collection_base_url}/{next_start}"

    @staticmethod
    def _metadata_to_multimap(metadata_entries):
        metadata_map = {}
        if not isinstance(metadata_entries, list):
            return metadata_map

        for entry in metadata_entries:
            if not isinstance(entry, dict):
                continue

            label = str(entry.get("label", "")).strip().lower()
            value = ChicagoLunaHarvester._extract_label(entry.get("value"))
            if not label or not value:
                continue

            metadata_map.setdefault(label, []).append(value)

        return metadata_map

    @staticmethod
    def _extract_first_image_resource(images):
        if not isinstance(images, list):
            return {}

        for image in images:
            if not isinstance(image, dict):
                continue
            resource = image.get("resource")
            if isinstance(resource, dict):
                return resource

        return {}

    @staticmethod
    def _extract_label(value):
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, list):
            return next((str(item).strip() for item in value if str(item).strip()), "")
        if isinstance(value, dict):
            for candidate in value.values():
                text = ChicagoLunaHarvester._extract_label(candidate)
                if text:
                    return text
        return ""

    @staticmethod
    def _extract_first_url(value):
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, list):
            for item in value:
                url = ChicagoLunaHarvester._extract_first_url(item)
                if url:
                    return url
        if isinstance(value, dict):
            for key in ("id", "@id", "href"):
                if value.get(key):
                    return str(value[key]).strip()
        return ""

    @staticmethod
    def _extract_thumbnail(value):
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, list):
            for item in value:
                thumb = ChicagoLunaHarvester._extract_thumbnail(item)
                if thumb:
                    return thumb
        if isinstance(value, dict):
            if value.get("id"):
                return str(value["id"]).strip()
            if value.get("@id"):
                return str(value["@id"]).strip()
        return ""

    @staticmethod
    def _extract_resource_id(value):
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, dict):
            if value.get("id"):
                return str(value["id"]).strip()
            if value.get("@id"):
                return str(value["@id"]).strip()
        return ""

    @staticmethod
    def _build_record_id(source_id):
        source_id = str(source_id or "").strip()
        if not source_id:
            return "chicago-luna-unknown"

        path = urlparse(source_id).path.rstrip("/")
        tail = path.split("/")[-1] if path else ""
        slug = re.sub(r"[^a-z0-9]+", "-", tail.lower()).strip("-")
        if slug:
            return f"chicago-luna-{slug}"

        digest = hashlib.sha1(source_id.encode("utf-8")).hexdigest()[:12]
        return f"chicago-luna-{digest}"

    @staticmethod
    def _series_or_default(df, column_name, default=""):
        if column_name in df.columns:
            return df[column_name].fillna(default)
        return pd.Series([default] * len(df), index=df.index, dtype="string")
