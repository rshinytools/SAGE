#!/usr/bin/env python
# SAGE - Factory 2: Metadata Pipeline
# ====================================
# Processes Excel specifications into Golden Metadata
"""
Metadata Factory Pipeline (Factory 2)

This script orchestrates the metadata processing pipeline:
1. Parse Excel specification files
2. Merge codelists with variable definitions
3. Generate plain-English descriptions using LLM
4. Store metadata for human review
5. Export approved golden metadata

Usage:
    python scripts/factory2_metadata.py --input specs/raw --output knowledge/golden_metadata.json
    python scripts/factory2_metadata.py --input specs/MySpec.xlsx --draft
    python scripts/factory2_metadata.py --export-approved
"""

import argparse
import logging
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import List, Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.metadata import (
    ExcelParser,
    CodelistMerger,
    MetadataStore,
    LLMDrafter,
    TemplateDrafter,
    DraftRequest,
    ChangeType
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MetadataPipeline:
    """
    Factory 2: Metadata Processing Pipeline.

    Processes Excel specification files through:
    1. Parsing - Extract domains, variables, codelists
    2. Merging - Link codelists to variables
    3. Drafting - Generate plain-English descriptions
    4. Storing - Save for review and approval
    5. Exporting - Export approved golden metadata
    """

    def __init__(
        self,
        storage_path: str = "knowledge/golden_metadata.json",
        version_db: str = "knowledge/metadata_versions.db",
        use_llm: bool = True
    ):
        """
        Initialize the metadata pipeline.

        Args:
            storage_path: Path for golden metadata JSON
            version_db: Path for version control database
            use_llm: Whether to use LLM for drafting descriptions
        """
        self.parser = ExcelParser()
        self.merger = CodelistMerger()
        self.store = MetadataStore(storage_path, version_db)

        # Initialize drafter
        if use_llm:
            self.drafter = LLMDrafter()
            if not self.drafter.is_available():
                logger.warning("LLM not available, falling back to template drafter")
                self.drafter = TemplateDrafter()
        else:
            self.drafter = TemplateDrafter()

        self.results = {
            'files_processed': 0,
            'domains_imported': 0,
            'variables_imported': 0,
            'codelists_imported': 0,
            'descriptions_drafted': 0,
            'errors': [],
            'warnings': []
        }

    def process_file(
        self,
        filepath: str,
        user: str = "system",
        draft_descriptions: bool = False
    ) -> bool:
        """
        Process a single Excel specification file.

        Args:
            filepath: Path to Excel file
            user: User performing the import
            draft_descriptions: Whether to generate LLM descriptions

        Returns:
            True if successful
        """
        filepath = Path(filepath)
        logger.info(f"Processing: {filepath}")

        # Step 1: Parse Excel file
        parse_result = self.parser.parse_file(str(filepath))

        if not parse_result.success:
            self.results['errors'].extend(parse_result.errors)
            logger.error(f"Failed to parse {filepath}: {parse_result.errors}")
            return False

        logger.info(f"  Parsed {len(parse_result.domains)} domains, "
                   f"{len(parse_result.codelists)} codelists")

        # Step 2: Add codelists to merger
        self.merger.add_codelists(parse_result.codelists)

        # Step 3: Merge codelists with domains
        merge_result = self.merger.merge_domains(parse_result.domains)

        if merge_result.warnings:
            self.results['warnings'].extend(merge_result.warnings)
            for warning in merge_result.warnings:
                logger.warning(f"  {warning}")

        logger.info(f"  Merged: {merge_result.statistics}")

        # Step 4: Import into metadata store
        self.store.import_merge_result(merge_result, user=user)
        self.store.import_codelists(parse_result.codelists, user=user)

        # Step 5: Generate descriptions if requested
        if draft_descriptions:
            self._draft_descriptions(merge_result, user)

        # Update results
        self.results['files_processed'] += 1
        self.results['domains_imported'] += len(merge_result.domains)
        self.results['variables_imported'] += merge_result.statistics.get('total_variables', 0)
        self.results['codelists_imported'] += len(parse_result.codelists)

        return True

    def process_directory(
        self,
        directory: str,
        user: str = "system",
        draft_descriptions: bool = False
    ) -> int:
        """
        Process all Excel files in a directory.

        Args:
            directory: Directory containing Excel files
            user: User performing the import
            draft_descriptions: Whether to generate LLM descriptions

        Returns:
            Number of files processed successfully
        """
        directory = Path(directory)

        if not directory.exists():
            logger.error(f"Directory not found: {directory}")
            return 0

        # Find Excel files
        excel_files = list(directory.glob("*.xlsx")) + \
                      list(directory.glob("*.xls")) + \
                      list(directory.glob("*.xlsm"))

        if not excel_files:
            logger.warning(f"No Excel files found in {directory}")
            return 0

        logger.info(f"Found {len(excel_files)} Excel files in {directory}")

        success_count = 0
        for filepath in excel_files:
            try:
                if self.process_file(filepath, user, draft_descriptions):
                    success_count += 1
            except Exception as e:
                logger.error(f"Error processing {filepath}: {e}")
                self.results['errors'].append(f"{filepath}: {e}")

        return success_count

    def _draft_descriptions(self, merge_result, user: str):
        """Generate plain-English descriptions for complex variables."""
        total_drafted = 0

        for enriched_domain in merge_result.domains:
            domain_name = enriched_domain.domain.name

            # Get variables that need drafting (have derivations)
            requests = []
            for ev in enriched_domain.variables:
                if ev.variable.derivation:
                    requests.append(DraftRequest(
                        variable_name=ev.variable.name,
                        domain=domain_name,
                        label=ev.variable.label,
                        derivation=ev.variable.derivation,
                        description=ev.variable.description,
                        codelist=ev.codelist_name,
                        codelist_values=ev.codelist_values,
                        data_type=ev.variable.data_type
                    ))

            if not requests:
                continue

            logger.info(f"  Drafting descriptions for {len(requests)} variables in {domain_name}")

            # Draft descriptions
            results = self.drafter.draft_batch(requests)

            # Update store with drafted descriptions
            for result in results:
                if result.plain_english and not result.error:
                    self.store.update_variable(
                        domain=result.domain,
                        name=result.variable_name,
                        updates={'plain_english': result.plain_english},
                        user=user
                    )
                    total_drafted += 1

        self.results['descriptions_drafted'] = total_drafted
        logger.info(f"  Drafted {total_drafted} descriptions")

    def save(self, user: str = "system", comment: str = ""):
        """Save metadata to storage."""
        self.store.save(user=user, comment=comment or "Metadata pipeline import")
        logger.info("Saved metadata to storage")

    def export_approved(self, output_path: Optional[str] = None) -> str:
        """Export only approved metadata."""
        path = self.store.export_golden_metadata(
            output_path=output_path,
            approved_only=True
        )
        logger.info(f"Exported approved metadata to {path}")
        return path

    def export_all(self, output_path: Optional[str] = None) -> str:
        """Export all metadata (including pending)."""
        path = self.store.export_golden_metadata(
            output_path=output_path,
            approved_only=False
        )
        logger.info(f"Exported all metadata to {path}")
        return path

    def get_summary(self) -> dict:
        """Get pipeline execution summary."""
        store_stats = self.store.get_statistics()
        approval_stats = self.store.get_approval_stats()

        return {
            'pipeline_results': self.results,
            'store_statistics': store_stats,
            'approval_status': approval_stats,
            'timestamp': datetime.now().isoformat()
        }

    def print_summary(self):
        """Print pipeline execution summary."""
        summary = self.get_summary()

        print("\n" + "=" * 60)
        print("SAGE Metadata Pipeline - Execution Summary")
        print("=" * 60)

        print("\nPipeline Results:")
        print(f"  Files processed:        {summary['pipeline_results']['files_processed']}")
        print(f"  Domains imported:       {summary['pipeline_results']['domains_imported']}")
        print(f"  Variables imported:     {summary['pipeline_results']['variables_imported']}")
        print(f"  Codelists imported:     {summary['pipeline_results']['codelists_imported']}")
        print(f"  Descriptions drafted:   {summary['pipeline_results']['descriptions_drafted']}")

        if summary['pipeline_results']['errors']:
            print(f"\n  Errors: {len(summary['pipeline_results']['errors'])}")
            for err in summary['pipeline_results']['errors'][:5]:
                print(f"    - {err}")

        if summary['pipeline_results']['warnings']:
            print(f"\n  Warnings: {len(summary['pipeline_results']['warnings'])}")
            for warn in summary['pipeline_results']['warnings'][:5]:
                print(f"    - {warn}")

        print("\nStore Statistics:")
        stats = summary['store_statistics']
        print(f"  Total domains:          {stats.get('total_domains', 0)}")
        print(f"  Total variables:        {stats.get('total_variables', 0)}")
        print(f"  Total codelists:        {stats.get('total_codelists', 0)}")
        print(f"  Approval percentage:    {stats.get('approval_percentage', 0):.1f}%")

        print("\nApproval Status:")
        for entity_type, counts in summary['approval_status'].items():
            pending = counts.get('pending', 0)
            approved = counts.get('approved', 0)
            rejected = counts.get('rejected', 0)
            print(f"  {entity_type.capitalize():12s} "
                  f"Pending: {pending:4d}  "
                  f"Approved: {approved:4d}  "
                  f"Rejected: {rejected:4d}")

        print("\n" + "=" * 60)


def main():
    """Main entry point for the metadata pipeline."""
    parser = argparse.ArgumentParser(
        description='SAGE Metadata Pipeline - Process Excel specifications into Golden Metadata'
    )

    parser.add_argument(
        '--input', '-i',
        type=str,
        help='Input Excel file or directory containing Excel files'
    )

    parser.add_argument(
        '--output', '-o',
        type=str,
        default='knowledge/golden_metadata.json',
        help='Output path for golden metadata JSON'
    )

    parser.add_argument(
        '--version-db',
        type=str,
        default='knowledge/metadata_versions.db',
        help='Path to version control database'
    )

    parser.add_argument(
        '--user', '-u',
        type=str,
        default='system',
        help='User performing the import'
    )

    parser.add_argument(
        '--draft',
        action='store_true',
        help='Generate LLM descriptions for complex derivations'
    )

    parser.add_argument(
        '--no-llm',
        action='store_true',
        help='Use template-based descriptions instead of LLM'
    )

    parser.add_argument(
        '--export-approved',
        action='store_true',
        help='Export only approved metadata'
    )

    parser.add_argument(
        '--export-all',
        action='store_true',
        help='Export all metadata including pending'
    )

    parser.add_argument(
        '--summary',
        action='store_true',
        help='Print store summary and statistics'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Initialize pipeline
    pipeline = MetadataPipeline(
        storage_path=args.output,
        version_db=args.version_db,
        use_llm=not args.no_llm
    )

    # Process input files
    if args.input:
        input_path = Path(args.input)

        if input_path.is_file():
            pipeline.process_file(
                str(input_path),
                user=args.user,
                draft_descriptions=args.draft
            )
        elif input_path.is_dir():
            pipeline.process_directory(
                str(input_path),
                user=args.user,
                draft_descriptions=args.draft
            )
        else:
            logger.error(f"Input not found: {input_path}")
            sys.exit(1)

        # Save results
        pipeline.save(user=args.user, comment=f"Import from {input_path}")

    # Export operations
    if args.export_approved:
        pipeline.export_approved()

    if args.export_all:
        pipeline.export_all()

    # Print summary
    if args.summary or args.input:
        pipeline.print_summary()

    return 0


if __name__ == '__main__':
    sys.exit(main())
