


function promptEditConnection(host, tailscalePeers) {
    const current = `${host.name}:${host.port}`;
    console.log("Current broker address:", current);
    return new Promise((resolve) => {
        const dialog = document.getElementById('inputDialog');
        const input = document.getElementById('dialogInput');

        let selectedPeer = null;

        input.value = ''; // Reset input
        if (tailscalePeers) {
            const peerInfo = document.getElementById('tailscalePeerInfo');
            peerInfo.innerHTML = ''; // Clear existing options
            const defaultOption = document.createElement('option');
            defaultOption.textContent = 'Select a peer';
            defaultOption.disabled = true;
            defaultOption.selected = true;
            peerInfo.appendChild(defaultOption);
            for (const peer of tailscalePeers.sort((a, b) => a.HostName.localeCompare(b.HostName))) {
                const option = document.createElement('option');
                option.className = 'peer-info';
                option.textContent = `${peer.HostName} - ${peer.TailscaleIPs[0]}`;
                peerInfo.appendChild(option);
            }

            peerInfo.addEventListener('change', (event) => {
                selectedPeer = event.target.value.split(' - ')[1];
            });
        }
        dialog.showModal();

        dialog.onclose = () => {
            console.log("Dialog closed with returnValue:", dialog.returnValue);
            if (dialog.returnValue === 'cancel') {
                resolve(null); // User cancelled
            } else {
                if (selectedPeer) {
                    if (selectedPeer !== 'Select a peer') {
                        console.log("Selected peer:", selectedPeer);
                        input.value = selectedPeer;
                    }
                }

                console.log("User submitted new connection:", input.value);
                resolve(input.value); // User submitted text
            }
        };
    });
}


export { promptEditConnection };