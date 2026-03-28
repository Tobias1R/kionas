import * as React from 'react';
import ReactDOM from 'react-dom';
import { styled } from '@mui/material/styles';
import { useRef } from 'react';
import FramedDemo from './FramedDemo';

const Frame = styled('iframe')(({ theme }) => ({
	backgroundColor: theme.vars.palette.background.default,
	flexGrow: 1,
	height: 400,
	border: 0,
	boxShadow: theme.shadows[1]
}));

type DemoFrameProps = {
	name: string;
	children: React.ReactElement;
	other?: React.HTMLAttributes<HTMLElement>;
};

/**
 * DemoFrame component for creating styled iframe
 */
function DemoFrame(props: DemoFrameProps) {
	const { children, name, ...other } = props;
	const title = `${name} demo`;

	const frameRef = useRef<HTMLIFrameElement>(null);

	// If we load portal content into the iframe before the load event then that content
	// is dropped in firefox.
	const [iframeLoaded, onLoad] = React.useReducer(() => true, false);
	const [frameDocument, setFrameDocument] = React.useState<Document | null>(null);

	const handleLoad = React.useCallback(() => {
		const nextDocument = frameRef.current?.contentDocument ?? null;

		if (nextDocument) {
			setFrameDocument(nextDocument);
		}

		onLoad();
	}, [onLoad]);

	React.useEffect(() => {
		const document = frameRef.current?.contentDocument;

		// When we hydrate the iframe then the load event is already dispatched
		// once the iframe markup is parsed (maybe later but the important part is
		// that it happens before React can attach event listeners).
		// We need to check the readyState of the document once the iframe is mounted
		// and "replay" the missed load event.
		// See https://github.com/facebook/react/pull/13862 for ongoing effort in React
		// (though not with iframes in mind).
		if (document != null && document.readyState === 'complete' && !iframeLoaded) {
			setFrameDocument(document);
			onLoad();
		}
	}, [iframeLoaded]);

	return (
		<>
			<Frame
				onLoad={handleLoad}
				ref={frameRef}
				title={title}
				{...other}
			/>
			{iframeLoaded !== false && frameDocument
				? ReactDOM.createPortal(
						<FramedDemo document={frameDocument}>{children}</FramedDemo>,
						frameDocument.body
					)
				: null}
		</>
	);
}

export default DemoFrame;
